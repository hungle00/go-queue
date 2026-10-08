package main

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StreamName   = "jobs"
	GroupName    = "workers"
	DLStreamName = "jobs:dead"
	MaxAttempts  = 3
	StatusPrefix = "job-queue:status"
	JobPrefix    = "job-queue:job"
	DelayedKey   = "job-queue:delayed"
)

type JobQueue struct {
	rdb *redis.Client
}

func NewJobQueue(rdb *redis.Client) *JobQueue {
	return &JobQueue{rdb: rdb}
}

func (jq *JobQueue) EnsureConsumerGroup(ctx context.Context) error {
	err := jq.rdb.XGroupCreateMkStream(ctx, StreamName, GroupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group: %w", err)
	}
	return nil
}

// Enqueue stores a job and adds its ID to the Redis Stream.
func (jq *JobQueue) Enqueue(ctx context.Context, jobType string, payload interface{}) (string, error) {
	job, err := NewJob(jobType, payload)
	if err != nil {
		return "", err
	}

	data, err := job.Serialize()
	if err != nil {
		return "", err
	}

	pipe := jq.rdb.TxPipeline()
	pipe.Set(ctx, jq.jobKey(job.ID), data, 0)
	pipe.SAdd(ctx, jq.statusKey(job.Status), job.ID)
	pipe.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamName,
		Values: map[string]interface{}{"job_id": job.ID},
	})
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("enqueue job: %w", err)
	}

	return job.ID, nil
}

// GetJob retrieves a job by its ID.
func (jq *JobQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	data, err := jq.rdb.Get(ctx, jq.jobKey(jobID)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return DeserializeJob(data)
}

type FetchedJob struct {
	MessageID string
	JobID     string
}

// FetchJobs reads new messages from the consumer group's stream.
func (jq *JobQueue) FetchJobs(ctx context.Context, consumerName string, count int64, block time.Duration) ([]FetchedJob, error) {
	entries, err := jq.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    GroupName,
		Consumer: consumerName,
		Streams:  []string{StreamName, ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	var fetched []FetchedJob
	for _, stream := range entries {
		for _, message := range stream.Messages {
			jobID, ok := message.Values["job_id"].(string)
			if !ok {
				jobID = ""
			}
			fetched = append(fetched, FetchedJob{MessageID: message.ID, JobID: jobID})
		}
	}
	return fetched, nil
}

// RetryJob schedules another attempt with exponential backoff or sends the job to the dead-letter stream.
func (jq *JobQueue) RetryJob(ctx context.Context, job *Job, messageID string, jobErr error, baseDelay time.Duration) error {
	updated := *job
	updated.Attempts++
	updated.Error = "max retries exceeded"
	if jobErr != nil {
		updated.Error = jobErr.Error()
	}

	oldStatus := job.Status
	pipe := jq.rdb.TxPipeline()
	pipe.SRem(ctx, jq.statusKey(oldStatus), job.ID)

	if updated.Attempts >= MaxAttempts {
		updated.Status = StatusDeadLetter
	} else {
		updated.Status = StatusRetrying
	}

	data, err := updated.Serialize()
	if err != nil {
		return err
	}
	pipe.SAdd(ctx, jq.statusKey(updated.Status), job.ID)
	pipe.Set(ctx, jq.jobKey(job.ID), data, 0)

	if updated.Status == StatusDeadLetter {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: DLStreamName,
			Values: map[string]interface{}{
				"job_id":   job.ID,
				"attempts": updated.Attempts,
				"error":    updated.Error,
			},
		})
		pipe.XAck(ctx, StreamName, GroupName, messageID)
	} else {
		delay := baseDelay * time.Duration(math.Pow(2, float64(updated.Attempts-1)))
		executeAt := time.Now().Add(delay).UnixMilli()
		pipe.ZAdd(ctx, DelayedKey, redis.Z{Score: float64(executeAt), Member: job.ID})
		pipe.XAck(ctx, StreamName, GroupName, messageID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("record failed job attempt: %w", err)
	}
	*job = updated
	return nil
}

// EnqueueScheduledJobs moves due retry jobs back to the stream.
func (jq *JobQueue) EnqueueScheduledJobs(ctx context.Context) (int, error) {
	const script = `
local ready = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
local enqueued = 0
for _, jobID in ipairs(ready) do
	if redis.call("ZREM", KEYS[1], jobID) == 1 then
		redis.call("XADD", KEYS[2], "*", "job_id", jobID)
		enqueued = enqueued + 1
	end
end
return enqueued
`
	count, err := jq.rdb.Eval(ctx, script, []string{DelayedKey, StreamName}, time.Now().UnixMilli(), 100).Int()
	if err != nil {
		return 0, fmt.Errorf("enqueue scheduled jobs: %w", err)
	}
	return count, nil
}

// GetJobsByStatus retrieves jobs indexed under the given status.
func (jq *JobQueue) GetJobsByStatus(ctx context.Context, status string) ([]*Job, error) {
	ids, err := jq.rdb.SMembers(ctx, jq.statusKey(status)).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0, len(ids))
	for _, jobID := range ids {
		job, err := jq.GetJob(ctx, jobID)
		if err != nil {
			return nil, fmt.Errorf("get job %s: %w", jobID, err)
		}
		if job != nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

// ReclaimStale claims pending messages that have been idle for at least minIdleTime.
func (jq *JobQueue) ReclaimStale(ctx context.Context, consumerName string, minIdleTime time.Duration, start string) ([]FetchedJob, string, error) {
	messages, next, err := jq.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   StreamName,
		Group:    GroupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Start:    start,
		Count:    10,
	}).Result()
	if err != nil {
		return nil, start, err
	}

	reclaimed := make([]FetchedJob, 0, len(messages))
	for _, message := range messages {
		jobID, ok := message.Values["job_id"].(string)
		if !ok {
			jobID = ""
		}
		reclaimed = append(reclaimed, FetchedJob{MessageID: message.ID, JobID: jobID})
	}
	return reclaimed, next, nil
}

// Ack acknowledges a processed stream message.
func (jq *JobQueue) Ack(ctx context.Context, messageID string) error {
	return jq.rdb.XAck(ctx, StreamName, GroupName, messageID).Err()
}

// UpdateStatus atomically updates a job record and its status index.
func (jq *JobQueue) UpdateStatus(ctx context.Context, job *Job, newStatus string) error {
	updated := *job
	updated.Status = newStatus
	data, err := updated.Serialize()
	if err != nil {
		return err
	}

	pipe := jq.rdb.TxPipeline()
	pipe.SRem(ctx, jq.statusKey(job.Status), job.ID)
	pipe.SAdd(ctx, jq.statusKey(newStatus), job.ID)
	pipe.Set(ctx, jq.jobKey(job.ID), data, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("update job %s status: %w", job.ID, err)
	}

	job.Status = newStatus
	return nil
}

func (jq *JobQueue) jobKey(jobID string) string {
	return fmt.Sprintf("%s:%s", JobPrefix, jobID)
}

func (jq *JobQueue) statusKey(status string) string {
	return fmt.Sprintf("%s:%s", StatusPrefix, status)
}
