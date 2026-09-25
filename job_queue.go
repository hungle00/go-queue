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
	jq := &JobQueue{rdb: rdb}
	jq.createConsumerGroup(context.Background())
	return jq
}

// Enqueue đẩy job mới vào Redis Stream
func (jq *JobQueue) Enqueue(ctx context.Context, jobType string, payload interface{}) (string, error) {
	job, err := NewJob(jobType, payload)
	if err != nil {
		return "", err
	}

	if err := jq.saveJob(ctx, job); err != nil {
		return "", err
	}

	// SADD status
	jq.rdb.SAdd(ctx, jq.statusKey(job.Status), job.ID)

	// XADD stream
	msgID, err := jq.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: StreamName,
		Values: map[string]interface{}{"job_id": job.ID},
	}).Result()
	if err != nil {
		return "", err
	}

	jq.UpdateStatus(ctx, job, StatusQueued)

	fmt.Printf("Enqueued job=%s, message=%s\n", job.ID, msgID)
	return job.ID, nil
}

// GetJob lấy thông tin Job bằng job_id
func (jq *JobQueue) GetJob(ctx context.Context, jobID string) (*Job, error) {
	data, err := jq.rdb.Get(ctx, jq.jobKey(jobID)).Result()
	if err == redis.Nil {
		return nil, nil // Job không tồn tại
	} else if err != nil {
		return nil, err
	}

	return DeserializeJob(data)
}

// FetchJobs lấy danh sách message từ Redis Stream
type FetchedJob struct {
	MessageID string
	JobID     string
}

func (jq *JobQueue) FetchJobs(ctx context.Context, consumerName string, count int64, blockMs time.Duration) ([]FetchedJob, error) {
	for {
		entries, err := jq.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    GroupName,
			Consumer: consumerName,
			Streams:  []string{StreamName, ">"},
			Count:    count,
			Block:    blockMs,
		}).Result()

		if err == redis.Nil || len(entries) == 0 {
			fmt.Println("No new messages. Waiting...")
			return nil, nil
		} else if err != nil {
			return nil, err
		}

		var parsedJobs []FetchedJob
		for _, stream := range entries {
			for _, message := range stream.Messages {
				jobID, _ := message.Values["job_id"].(string)
				parsedJobs = append(parsedJobs, FetchedJob{
					MessageID: message.ID,
					JobID:     jobID,
				})
			}
		}

		return parsedJobs, nil
	}
}

// RetryJob thực hiện retry theo Exponential Backoff hoặc chuyển sang Dead Letter Queue
func (jq *JobQueue) RetryJob(ctx context.Context, job *Job, messageID string, jobErr error, baseDelay float64) error {
	attempts := job.Attempts + 1
	job.Attempts = attempts

	errMsg := "max retries exceeded"
	if jobErr != nil {
		errMsg = jobErr.Error()
	}
	job.Error = errMsg

	// Nếu quá số lần retry tối đa -> Đẩy sang DLQ
	if attempts >= MaxAttempts {
		job.Status = StatusDeadLetter
		jq.saveJob(ctx, job)
		jq.UpdateStatus(ctx, job, StatusDeadLetter)

		jq.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: DLStreamName,
			Values: map[string]interface{}{
				"job_id":   job.ID,
				"attempts": attempts,
				"error":    job.Error,
			},
		})
		jq.Ack(ctx, messageID)
		fmt.Printf("Job %s moved to DLQ\n", job.ID)
		return nil
	}

	// Calculate exponential backoff delay
	delaySeconds := baseDelay * math.Pow(2, float64(attempts-1))
	executeAt := float64(time.Now().Unix()) + delaySeconds

	job.Status = StatusRetrying
	jq.saveJob(ctx, job)
	jq.UpdateStatus(ctx, job, StatusRetrying)

	// Thêm vào Redis ZSET
	jq.rdb.ZAdd(ctx, DelayedKey, redis.Z{
		Score:  executeAt,
		Member: job.ID,
	})
	jq.Ack(ctx, messageID)

	fmt.Printf("Retry job=%s, attempt=%d\n", job.ID, attempts)
	return nil
}

// EnqueueScheduledJobs quét ZSET tìm các job đã tới giờ chạy và đẩy lại Stream
func (jq *JobQueue) EnqueueScheduledJobs(ctx context.Context) (int, error) {
	now := float64(time.Now().Unix())

	readyJobs, err := jq.rdb.ZRangeByScore(ctx, DelayedKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%f", now),
	}).Result()

	if err != nil || len(readyJobs) == 0 {
		return 0, err
	}

	// Dùng Pipeline trong Go
	pipe := jq.rdb.Pipeline()
	for _, jobID := range readyJobs {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: StreamName,
			Values: map[string]interface{}{"job_id": jobID},
		})
		pipe.ZRem(ctx, DelayedKey, jobID)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}

	return len(readyJobs), nil
}

// GetJobsByStatus lấy danh sách job theo trạng thái
func (jq *JobQueue) GetJobsByStatus(ctx context.Context, status string) ([]*Job, error) {
	ids, err := jq.rdb.SMembers(ctx, jq.statusKey(status)).Result()
	if err != nil {
		return nil, err
	}

	var jobs []*Job
	for _, jobID := range ids {
		job, err := jq.GetJob(ctx, jobID)
		if err == nil && job != nil {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

// ReclaimStale tự động nhận lại các message bị treo (pending) quá thời gian quy định
func (jq *JobQueue) ReclaimStale(ctx context.Context, consumerName string, minIdleTime time.Duration) error {
	messages, _, err := jq.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   StreamName,
		Group:    GroupName,
		Consumer: consumerName,
		MinIdle:  minIdleTime,
		Start:    "0-0",
		Count:    10,
	}).Result()

	if err != nil {
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	fmt.Printf("Reclaimed %d stale messages\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("Processing stale message: %s\n", msg.ID)
	}
	return nil
}

// Ack xác nhận đã xử lý xong message
func (jq *JobQueue) Ack(ctx context.Context, messageID string) {
	jq.rdb.XAck(ctx, StreamName, GroupName, messageID)
}

// UpdateStatus cập nhật Set chứa trạng thái Job
func (jq *JobQueue) UpdateStatus(ctx context.Context, job *Job, newStatus string) {
	oldStatus := job.Status
	jq.rdb.SRem(ctx, jq.statusKey(oldStatus), job.ID)
	jq.rdb.SAdd(ctx, jq.statusKey(newStatus), job.ID)
	job.Status = newStatus
	jq.saveJob(ctx, job)
}

func (jq *JobQueue) createConsumerGroup(ctx context.Context) {
	err := jq.rdb.XGroupCreateMkStream(ctx, StreamName, GroupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		fmt.Printf("XGroupCreate error: %v\n", err)
	}
}

func (jq *JobQueue) jobKey(jobID string) string {
	return fmt.Sprintf("%s:%s", JobPrefix, jobID)
}

func (jq *JobQueue) statusKey(status string) string {
	return fmt.Sprintf("%s:%s", StatusPrefix, status)
}

func (jq *JobQueue) saveJob(ctx context.Context, job *Job) error {
	data, err := job.Serialize()
	if err != nil {
		return err
	}
	return jq.rdb.Set(ctx, jq.jobKey(job.ID), data, 0).Err()
}
