// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package redis adapts Asynq's Redis storage implementation to the pluggable
// Backend interface.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/errors"
	"github.com/hibiken/asynq/internal/rdb"
	goredis "github.com/redis/go-redis/v9"
)

// Backend stores tasks in Redis.
type Backend struct {
	broker *rdb.RDB
}

// NewBackend returns a Redis-backed pluggable backend.
func NewBackend(opt asynq.RedisConnOpt) *Backend {
	client, ok := opt.MakeRedisClient().(goredis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", opt))
	}
	return NewBackendFromRedisClient(client)
}

// NewBackendFromRedisClient returns a Redis backend using an existing client.
func NewBackendFromRedisClient(client goredis.UniversalClient) *Backend {
	return &Backend{broker: rdb.NewRDB(client)}
}

// Ping performs a ping against Redis.
func (b *Backend) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return b.broker.Ping()
}

// Close closes the Redis connection.
func (b *Backend) Close() error {
	return b.broker.Close()
}

// Enqueue stores msg as pending or scheduled depending on msg.RunAt.
func (b *Backend) Enqueue(ctx context.Context, msg *asynq.TaskMessage) error {
	internal := toInternal(msg)
	if msg.RunAt.After(time.Now()) {
		return b.broker.Schedule(ctx, internal, msg.RunAt)
	}
	return b.broker.Enqueue(ctx, internal)
}

// Dequeue returns the next processable task from queues.
func (b *Backend) Dequeue(ctx context.Context, queues []string) (*asynq.TaskMessage, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	msg, _, err := b.broker.Dequeue(queues...)
	if errors.Is(err, errors.ErrNoProcessableTask) {
		return nil, asynq.ErrNoProcessableTask
	}
	if err != nil {
		return nil, err
	}
	return fromInternal(msg, time.Now()), nil
}

// Done marks msg as processed successfully.
func (b *Backend) Done(ctx context.Context, msg *asynq.TaskMessage) error {
	return b.broker.Done(ctx, toInternal(msg))
}

// Retry schedules msg to be processed again at processAt.
func (b *Backend) Retry(ctx context.Context, msg *asynq.TaskMessage, processAt time.Time, errMsg string) error {
	return b.broker.Retry(ctx, toInternal(msg), processAt, errMsg, true)
}

// Archive moves msg to the archive state.
func (b *Backend) Archive(ctx context.Context, msg *asynq.TaskMessage, errMsg string) error {
	return b.broker.Archive(ctx, toInternal(msg), errMsg)
}

func fromInternal(msg *base.TaskMessage, runAt time.Time) *asynq.TaskMessage {
	return asynq.TaskMessageFromInternal(msg, runAt)
}

func toInternal(msg *asynq.TaskMessage) *base.TaskMessage {
	if msg == nil {
		return nil
	}
	return &base.TaskMessage{
		ID:           msg.ID,
		Type:         msg.Type,
		Payload:      msg.Payload,
		Headers:      msg.Headers,
		Queue:        msg.Queue,
		Retry:        msg.Retry,
		Retried:      msg.Retried,
		ErrorMsg:     msg.ErrorMsg,
		LastFailedAt: unixOrZero(msg.LastFailedAt),
		Timeout:      int64(msg.Timeout.Seconds()),
		Deadline:     unixOrZero(msg.Deadline),
		UniqueKey:    msg.UniqueKey,
		GroupKey:     msg.GroupKey,
		Retention:    int64(msg.Retention.Seconds()),
		CompletedAt:  unixOrZero(msg.CompletedAt),
	}
}

func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}
