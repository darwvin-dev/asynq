// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"time"

	"github.com/hibiken/asynq/internal/base"
)

// ErrFeatureNotSupported indicates that a backend does not support the requested feature.
var ErrFeatureNotSupported = errors.New("asynq: feature is not supported by backend")

// ErrNoProcessableTask indicates that a backend has no task ready for processing.
var ErrNoProcessableTask = errors.New("asynq: no processable task")

// TaskMessage is the storage-facing representation used by pluggable backends.
type TaskMessage struct {
	ID           string
	Type         string
	Payload      []byte
	Headers      map[string]string
	Queue        string
	Retry        int
	Retried      int
	ErrorMsg     string
	LastFailedAt time.Time
	Timeout      time.Duration
	Deadline     time.Time
	UniqueKey    string
	GroupKey     string
	Retention    time.Duration
	CompletedAt  time.Time
	RunAt        time.Time
}

// Backend defines the minimum contract required to enqueue and process tasks
// without depending on Redis-specific internals.
type Backend interface {
	Ping(ctx context.Context) error
	Close() error
	Enqueue(ctx context.Context, msg *TaskMessage) error
	Dequeue(ctx context.Context, queues []string) (*TaskMessage, error)
	Done(ctx context.Context, msg *TaskMessage) error
	Retry(ctx context.Context, msg *TaskMessage, processAt time.Time, errMsg string) error
	Archive(ctx context.Context, msg *TaskMessage, errMsg string) error
}

// TaskMessageFromInternal converts Asynq's Redis-oriented task message into
// the public storage-facing message used by pluggable backends.
func TaskMessageFromInternal(msg *base.TaskMessage, runAt time.Time) *TaskMessage {
	if msg == nil {
		return nil
	}
	return &TaskMessage{
		ID:           msg.ID,
		Type:         msg.Type,
		Payload:      msg.Payload,
		Headers:      msg.Headers,
		Queue:        msg.Queue,
		Retry:        msg.Retry,
		Retried:      msg.Retried,
		ErrorMsg:     msg.ErrorMsg,
		LastFailedAt: fromUnixTimeOrZero(msg.LastFailedAt),
		Timeout:      time.Duration(msg.Timeout) * time.Second,
		Deadline:     fromUnixTimeOrZero(msg.Deadline),
		UniqueKey:    msg.UniqueKey,
		GroupKey:     msg.GroupKey,
		Retention:    time.Duration(msg.Retention) * time.Second,
		CompletedAt:  fromUnixTimeOrZero(msg.CompletedAt),
		RunAt:        runAt,
	}
}
