# Go Queue

A Go job queue backed by Redis Streams, Redis string keys, and a sorted set for delayed retries.

## Run producer and worker separately

Requirements: Go and Redis 6.2 or later available at `localhost:6379`.

Start Redis if it is not already running:

```sh
docker run --name go-queue-redis -p 6379:6379 -d redis:latest
```

In one terminal, enqueue the example jobs:

```sh
go run . -mode=producer
```

In another terminal, start the consumer:

```sh
go run . -mode=worker
```

The worker creates the Redis consumer group on startup, so it can be started before the producer. It runs with three concurrent job handlers and continues waiting for new jobs.

The example producer enqueues five `send_welcome_email` jobs each time it runs. Override the Redis address with `-redis-addr`. Each worker process gets a unique consumer name by default; set one explicitly with `-consumer` if needed. To start another worker process:

```sh
go run . -mode=worker -consumer=worker-2 -concurrency=3
```

## Queue behavior

- Jobs are persisted as JSON and referenced by ID in the Redis Stream.
- Handlers are registered by job type in each worker process.
- Failed jobs are retried with exponential delays and moved to the `jobs:dead` stream after three failed attempts.
- Workers reclaim messages that have been pending for at least one minute.
- Processing is at-least-once: a worker crash or a handler that runs longer than the reclaim timeout can cause a job to execute again. Job handlers should be idempotent.

## Check data on Redis
You can access Redis container and use redis-cli
```
docker exec -it go-queue-redis redis-cli
```
and use redis command
```
SMEMBERS job-queue:status:retrying
SMEMBERS job-queue:status:dead-letter
XRANGE jobs:dead - +
```
