package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

type EmailPayload struct {
	UserID int    `json:"user_id"`
	Email  string `json:"email"`
}

func main() {
	// Parse command line flags
	mode := flag.String("mode", "producer", "Mode to run: 'producer' or 'worker'")
	redisAddr := flag.String("redis-addr", "localhost:6379", "Redis server address")
	consumer := flag.String("consumer", fmt.Sprintf("worker-%d", os.Getpid()), "Worker consumer name")
	concurrency := flag.Int("concurrency", 3, "Number of concurrent worker handlers")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	switch *mode {
	case "producer":
		runProducer(rdb)
	case "worker":
		runWorker(rdb, *consumer, *concurrency)
	default:
		log.Fatalf("Unknown mode: %s. Use -mode=producer or -mode=worker", *mode)
	}
}

func runProducer(rdb *redis.Client) {
	ctx := context.Background()
	queue := NewJobQueue(rdb)

	for i := 1; i <= 5; i++ {
		payload := EmailPayload{UserID: 100 + i, Email: fmt.Sprintf("user%d@example.com", i)}
		jobID, err := queue.Enqueue(ctx, "send_welcome_email", payload)
		if err != nil {
			log.Printf("Failed to enqueue job %d: %v", i, err)
			continue
		}
		fmt.Printf("[Producer] Enqueued job %d with ID: %s\n", i, jobID)
	}
}

func runWorker(rdb *redis.Client, consumer string, concurrency int) {
	registry := NewJobRegistry()

	registry.Register("send_welcome_email", func(ctx context.Context, payload json.RawMessage) error {
		var p EmailPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		// Simulate a failure for demonstration purposes
		// if p.UserID == 101 {
    	// 	return fmt.Errorf("simulated failure")
		// }
		fmt.Printf("[Worker] Processing email for %s...\n", p.Email)
		time.Sleep(1 * time.Second)
		return nil
	})

	pool := NewWorkerPool(rdb, registry, consumer, concurrency)
	if err := pool.Start(); err != nil {
		log.Printf("Worker stopped with an error: %v", err)
	}
}
