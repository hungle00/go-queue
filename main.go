package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
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
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	switch *mode {
	case "producer":
		runProducer(rdb)
	case "worker":
		runWorker(rdb)
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

func runWorker(rdb *redis.Client) {
	registry := NewJobRegistry()

	registry.Register("send_welcome_email", func(ctx context.Context, payload json.RawMessage) error {
		var p EmailPayload
		if err := json.Unmarshal(payload, &p); err != nil {
			return err
		}
		fmt.Printf("[Worker] Processing email for %s...\n", p.Email)
		time.Sleep(1 * time.Second)
		return nil
	})

	pool := NewWorkerPool(rdb, registry, "worker-node-1", 3)
	pool.Start()
}
