// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package typed provides a small generic layer for dispatching and handling
// strongly typed Asynq task payloads.
package typed

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

const idempotencyKeyHeader = "asynq-idempotency-key"

// Option configures a typed dispatch operation.
type Option interface {
	apply(*dispatchOptions)
}

type dispatchOptions struct {
	asynqOptions []asynq.Option
}

type optionFunc func(*dispatchOptions)

func (fn optionFunc) apply(opts *dispatchOptions) {
	fn(opts)
}

// WithAsynqOptions passes regular Asynq enqueue options to Dispatch.
func WithAsynqOptions(opts ...asynq.Option) Option {
	return optionFunc(func(cfg *dispatchOptions) {
		cfg.asynqOptions = append(cfg.asynqOptions, opts...)
	})
}

// WithIdempotencyKey attaches a stable idempotency key to the task metadata.
//
// Backends can use this header to deduplicate dispatches once idempotency is
// enabled by the selected storage implementation.
func WithIdempotencyKey(key string) Option {
	return optionFunc(func(cfg *dispatchOptions) {
		cfg.asynqOptions = append(cfg.asynqOptions, asynq.Header(idempotencyKeyHeader, key))
	})
}

// Dispatch serializes payload as JSON and enqueues it under typename.
func Dispatch[T any](ctx context.Context, client *asynq.Client, typename string, payload T, opts ...Option) (*asynq.TaskInfo, error) {
	if client == nil {
		return nil, fmt.Errorf("typed: client cannot be nil")
	}
	var cfg dispatchOptions
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&cfg)
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("typed: marshal payload: %w", err)
	}
	return client.EnqueueContext(ctx, asynq.NewTask(typename, data), cfg.asynqOptions...)
}

// Register registers a typed handler on mux. The task payload is decoded from JSON.
func Register[T any](mux *asynq.ServeMux, typename string, handler func(context.Context, T) error) {
	if mux == nil {
		panic("typed: mux cannot be nil")
	}
	if handler == nil {
		panic("typed: handler cannot be nil")
	}
	mux.HandleFunc(typename, func(ctx context.Context, task *asynq.Task) error {
		var payload T
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			return fmt.Errorf("typed: unmarshal payload for %q: %w", typename, err)
		}
		return handler(ctx, payload)
	})
}
