package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

type WorkerPool struct {
	redisClient *redis.Client
	registry    *JobRegistry
	concurrency int
	consumer    string
	stream      string
	group       string
}

func NewWorkerPool(rdb *redis.Client, registry *JobRegistry, consumer string, concurrency int) *WorkerPool {
	return &WorkerPool{
		redisClient: rdb,
		registry:    registry,
		concurrency: concurrency,
		consumer:    consumer,
		stream:      "jobs",
		group:       "workers",
	}
}

func (wp *WorkerPool) Start() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Channel đóng vai trò như Task Queue nội bộ của Worker Pool
	jobChan := make(chan struct {
		msgID string
		job   *Job
	}, wp.concurrency)

	var wg sync.WaitGroup

	// 1. Khai báo Goroutine Worker Pool
	for i := 1; i <= wp.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range jobChan {
				wp.processJob(ctx, workerID, task.msgID, task.job)
			}
		}(i)
	}

	fmt.Printf("[Worker '%s'] Started with %d concurrent goroutines...\n", wp.consumer, wp.concurrency)

	// 2. Main Loop: Dequeue tin nhắn từ Redis Stream
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\n[Worker] Graceful shutdown signal received. Stopping fetch...")
			close(jobChan) // Đóng channel để các Goroutines dừng sau khi làm xong việc dở
			wg.Wait()      // Chờ toàn bộ Goroutine xử lý xong
			fmt.Println("[Worker] All workers stopped cleanly.")
			return
		default:
			// Fetch job từ Redis Stream (XREADGROUP)
			entries, err := wp.redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    wp.group,
				Consumer: wp.consumer,
				Streams:  []string{wp.stream, ">"},
				Count:    1,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					fmt.Printf("Error reading stream: %v\n", err)
				}
				continue
			}

			for _, stream := range entries {
				for _, message := range stream.Messages {
					jobID, ok := message.Values["job_id"].(string)
					if !ok {
						continue
					}

					jobData, err := wp.redisClient.Get(ctx, fmt.Sprintf("%s:%s", JobPrefix, jobID)).Result()
					if err != nil {
						fmt.Printf("Failed to load job %s: %v\n", jobID, err)
						continue
					}

					job, err := DeserializeJob(jobData)
					if err != nil {
						fmt.Printf("Failed to decode job: %v\n", err)
						continue
					}

					// Đẩy Job vào Channel để cho Goroutine trong Pool nhặt làm
					jobChan <- struct {
						msgID string
						job   *Job
					}{msgID: message.ID, job: job}
				}
			}
		}
	}
}

func (wp *WorkerPool) processJob(ctx context.Context, workerID int, msgID string, job *Job) {
	fmt.Printf("[Worker %d] Processing Job %s (Type: %s)\n", workerID, job.ID, job.Type)

	// Execute Job Handler
	err := wp.registry.Execute(ctx, job)
	if err != nil {
		fmt.Printf("[Worker %d] Job %s failed: %v\n", workerID, job.ID, err)
		// Xử lý Retry / Dead Letter Queue ở đây
		return
	}

	// ACK Message nếu thành công
	wp.redisClient.XAck(ctx, wp.stream, wp.group, msgID)
	fmt.Printf("[Worker %d] Job %s completed successfully.\n", workerID, job.ID)
}
