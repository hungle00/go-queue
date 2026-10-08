package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const staleMessageIdleTime = time.Minute

type jobTask struct {
	messageID string
	job       *Job
}

type WorkerPool struct {
	redisClient *redis.Client
	queue       *JobQueue
	registry    *JobRegistry
	concurrency int
	consumer    string
	stream      string
	group       string
}

func NewWorkerPool(rdb *redis.Client, registry *JobRegistry, consumer string, concurrency int) *WorkerPool {
	return &WorkerPool{
		redisClient: rdb,
		queue:       NewJobQueue(rdb),
		registry:    registry,
		concurrency: concurrency,
		consumer:    consumer,
		stream:      StreamName,
		group:       GroupName,
	}
}

func (wp *WorkerPool) Start() error {
	if wp.concurrency < 1 {
		return fmt.Errorf("worker concurrency must be positive")
	}
	if wp.registry == nil {
		return fmt.Errorf("worker registry must not be nil")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := wp.queue.EnsureConsumerGroup(ctx); err != nil {
		return err
	}

	workCtx := context.Background()
	jobChan := make(chan jobTask, wp.concurrency)
	var wg sync.WaitGroup

	for i := 1; i <= wp.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range jobChan {
				wp.processJob(workCtx, workerID, task.messageID, task.job)
			}
		}(i)
	}

	log.Printf("[Worker %q] Started with %d concurrent goroutines", wp.consumer, wp.concurrency)
	reclaimCursor := "0-0"

	dispatch := func(messages []FetchedJob) bool {
		for _, message := range messages {
			if message.JobID == "" {
				log.Printf("Stream message %s has no job_id; acknowledging it", message.MessageID)
				if err := wp.queue.Ack(ctx, message.MessageID); err != nil {
					log.Printf("Failed to acknowledge malformed message %s: %v", message.MessageID, err)
				}
				continue
			}

			job, err := wp.queue.GetJob(ctx, message.JobID)
			if err != nil {
				log.Printf("Failed to load job %s: %v", message.JobID, err)
				continue
			}
			if job == nil {
				log.Printf("Job %s does not exist; acknowledging message %s", message.JobID, message.MessageID)
				if err := wp.queue.Ack(ctx, message.MessageID); err != nil {
					log.Printf("Failed to acknowledge missing job message %s: %v", message.MessageID, err)
				}
				continue
			}
			if job.Status == StatusCompleted || job.Status == StatusDeadLetter {
				if err := wp.queue.Ack(ctx, message.MessageID); err != nil {
					log.Printf("Failed to acknowledge already-finished job %s: %v", job.ID, err)
				}
				continue
			}

			select {
			case jobChan <- jobTask{messageID: message.MessageID, job: job}:
			case <-ctx.Done():
				return false
			}
		}
		return true
	}

	for ctx.Err() == nil {
		if _, err := wp.queue.EnqueueScheduledJobs(ctx); err != nil && ctx.Err() == nil {
			log.Printf("Failed to enqueue scheduled jobs: %v", err)
		}

		reclaimed, nextCursor, err := wp.queue.ReclaimStale(ctx, wp.consumer, staleMessageIdleTime, reclaimCursor)
		if err != nil && ctx.Err() == nil {
			log.Printf("Failed to reclaim stale messages: %v", err)
		} else {
			reclaimCursor = nextCursor
			if !dispatch(reclaimed) {
				break
			}
		}

		messages, err := wp.queue.FetchJobs(ctx, wp.consumer, int64(wp.concurrency), time.Second)
		if err != nil && ctx.Err() == nil {
			log.Printf("Failed to read jobs: %v", err)
			continue
		}
		if ctx.Err() != nil || !dispatch(messages) {
			break
		}
	}

	log.Println("[Worker] Shutdown signal received; stopping fetch and draining queued work")
	close(jobChan)
	wg.Wait()
	log.Println("[Worker] All workers stopped")
	return nil
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID int, messageID string, job *Job) {
	log.Printf("[Worker %d] Processing job %s (type: %s)", workerID, job.ID, job.Type)

	if err := wp.queue.UpdateStatus(ctx, job, StatusProcessing); err != nil {
		log.Printf("[Worker %d] Failed to mark job %s as processing: %v", workerID, job.ID, err)
		return
	}

	if err := wp.registry.Execute(ctx, job); err != nil {
		log.Printf("[Worker %d] Job %s failed: %v", workerID, job.ID, err)
		if retryErr := wp.queue.RetryJob(ctx, job, messageID, err, time.Second); retryErr != nil {
			log.Printf("[Worker %d] Failed to schedule retry for job %s: %v", workerID, job.ID, retryErr)
		}
		return
	}

	job.Error = ""
	if err := wp.queue.UpdateStatus(ctx, job, StatusCompleted); err != nil {
		log.Printf("[Worker %d] Failed to mark job %s as completed: %v", workerID, job.ID, err)
		return
	}
	if err := wp.queue.Ack(ctx, messageID); err != nil {
		log.Printf("[Worker %d] Failed to acknowledge job %s: %v", workerID, job.ID, err)
		return
	}
	log.Printf("[Worker %d] Job %s completed successfully", workerID, job.ID)
}
