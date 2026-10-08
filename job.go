package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type JobHandler func(context.Context, json.RawMessage) error

type JobRegistry struct {
	mu       sync.RWMutex
	handlers map[string]JobHandler
}

func NewJobRegistry() *JobRegistry {
	return &JobRegistry{handlers: make(map[string]JobHandler)}
}

func (r *JobRegistry) Register(jobType string, handler JobHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[jobType] = handler
}

func (r *JobRegistry) Execute(ctx context.Context, job *Job) error {
	r.mu.RLock()
	handler, ok := r.handlers[job.Type]
	r.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no handler registered for job type %q", job.Type)
	}
	return handler(ctx, job.Payload)
}

// Job status constants.
const (
	StatusQueued     = "queued"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusRetrying   = "retrying"
	StatusDeadLetter = "dead-letter"
)

// Job represents a task.
type Job struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`    // The task type, used to select a handler.
	Payload  json.RawMessage `json:"payload"` // Dynamic task arguments encoded as JSON.
	Status   string          `json:"status"`
	Attempts int             `json:"attempts"`
	Error    string          `json:"error,omitempty"`
}

// NewJob creates a job.
func NewJob(jobType string, payload interface{}) (*Job, error) {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	return &Job{
		ID:       uuid.New().String(),
		Type:     jobType,
		Payload:  bytes,
		Status:   StatusQueued,
		Attempts: 0,
	}, nil
}

// Serialize encodes a job as JSON.
func (j *Job) Serialize() (string, error) {
	bytes, err := json.Marshal(j)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// DeserializeJob decodes a job from JSON.
func DeserializeJob(data string) (*Job, error) {
	var j Job
	err := json.Unmarshal([]byte(data), &j)
	if err != nil {
		return nil, err
	}
	return &j, nil
}
