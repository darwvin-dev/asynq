// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/log"
)

type backendRunner struct {
	logger  *log.Logger
	backend Backend
	state   *serverState

	baseCtxFn         func() context.Context
	retryDelayFunc    RetryDelayFunc
	isFailureFunc     func(error) bool
	errHandler        ErrorHandler
	taskCheckInterval time.Duration
	shutdownTimeout   time.Duration
	queues            []string

	sema  chan struct{}
	done  chan struct{}
	abort chan struct{}
	once  sync.Once
	wg    sync.WaitGroup
}

func newBackendRunner(logger *log.Logger, backend Backend, cfg Config, state *serverState) *backendRunner {
	baseCtxFn := cfg.BaseContext
	if baseCtxFn == nil {
		baseCtxFn = context.Background
	}
	n := cfg.Concurrency
	if n < 1 {
		n = runtime.NumCPU()
	}
	taskCheckInterval := cfg.TaskCheckInterval
	if taskCheckInterval <= 0 {
		taskCheckInterval = defaultTaskCheckInterval
	}
	retryDelayFunc := cfg.RetryDelayFunc
	if retryDelayFunc == nil {
		retryDelayFunc = DefaultRetryDelayFunc
	}
	isFailureFunc := cfg.IsFailure
	if isFailureFunc == nil {
		isFailureFunc = defaultIsFailureFunc
	}
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	queues := normalizeServerQueues(cfg.Queues)
	qnames := make([]string, 0, len(queues))
	for q := range queues {
		qnames = append(qnames, q)
	}
	return &backendRunner{
		logger:            logger,
		backend:           backend,
		state:             state,
		baseCtxFn:         baseCtxFn,
		retryDelayFunc:    retryDelayFunc,
		isFailureFunc:     isFailureFunc,
		errHandler:        cfg.ErrorHandler,
		taskCheckInterval: taskCheckInterval,
		shutdownTimeout:   shutdownTimeout,
		queues:            qnames,
		sema:              make(chan struct{}, n),
		done:              make(chan struct{}),
		abort:             make(chan struct{}),
	}
}

func (r *backendRunner) start(handler Handler) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.done:
				return
			default:
				r.exec(handler)
			}
		}
	}()
}

func (r *backendRunner) shutdown() {
	r.once.Do(func() {
		close(r.done)
		time.AfterFunc(r.shutdownTimeout, func() { close(r.abort) })
	})
	for i := 0; i < cap(r.sema); i++ {
		r.sema <- struct{}{}
	}
	r.wg.Wait()
}

func (r *backendRunner) exec(handler Handler) {
	select {
	case <-r.done:
		return
	case r.sema <- struct{}{}:
		msg, err := r.backend.Dequeue(r.baseCtxFn(), r.queues)
		switch {
		case errors.Is(err, ErrNoProcessableTask):
			jitter := rand.N(r.taskCheckInterval)
			time.Sleep(r.taskCheckInterval/2 + jitter)
			<-r.sema
			return
		case err != nil:
			r.logger.Errorf("Backend dequeue error: %v", err)
			<-r.sema
			return
		}

		r.wg.Add(1)
		go func() {
			defer func() {
				<-r.sema
				r.wg.Done()
			}()
			r.process(handler, msg)
		}()
	}
}

func (r *backendRunner) process(handler Handler, msg *TaskMessage) {
	ctx, cancel := r.taskContext(msg)
	defer cancel()
	task := newTask(msg.Type, msg.Payload, nil)
	task.headers = msg.Headers
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.perform(handler, ctx, task)
	}()
	select {
	case <-r.abort:
		r.retry(ctx, msg, context.Canceled)
	case <-ctx.Done():
		r.retry(ctx, msg, ctx.Err())
	case err := <-errCh:
		if err != nil {
			r.handleFailed(ctx, msg, task, err)
			return
		}
		msg.CompletedAt = time.Now()
		if err := r.backend.Done(ctx, msg); err != nil {
			r.logger.Errorf("Could not mark backend task id=%s as done: %v", msg.ID, err)
		}
	}
}

func (r *backendRunner) taskContext(msg *TaskMessage) (context.Context, context.CancelFunc) {
	ctx := r.baseCtxFn()
	if !msg.Deadline.IsZero() && msg.Timeout > 0 {
		timeoutDeadline := time.Now().Add(msg.Timeout)
		if msg.Deadline.Before(timeoutDeadline) {
			return context.WithDeadline(ctx, msg.Deadline)
		}
		return context.WithDeadline(ctx, timeoutDeadline)
	}
	if !msg.Deadline.IsZero() {
		return context.WithDeadline(ctx, msg.Deadline)
	}
	if msg.Timeout > 0 {
		return context.WithTimeout(ctx, msg.Timeout)
	}
	return context.WithTimeout(ctx, defaultTimeout)
}

func (r *backendRunner) handleFailed(ctx context.Context, msg *TaskMessage, task *Task, err error) {
	if r.errHandler != nil {
		r.errHandler.HandleError(ctx, task, err)
	}
	switch {
	case errors.Is(err, RevokeTask):
		if err := r.backend.Done(ctx, msg); err != nil {
			r.logger.Errorf("Could not revoke backend task id=%s: %v", msg.ID, err)
		}
	case msg.Retried >= msg.Retry || errors.Is(err, SkipRetry):
		r.archive(ctx, msg, err)
	default:
		r.retry(ctx, msg, err)
	}
}

func (r *backendRunner) retry(ctx context.Context, msg *TaskMessage, err error) {
	msg.Retried++
	msg.ErrorMsg = err.Error()
	msg.LastFailedAt = time.Now()
	retryAt := time.Now().Add(r.retryDelayFunc(msg.Retried, err, NewTaskWithHeaders(msg.Type, msg.Payload, msg.Headers)))
	if e := r.backend.Retry(ctx, msg, retryAt, err.Error()); e != nil {
		r.logger.Errorf("Could not retry backend task id=%s: %v", msg.ID, e)
	}
}

func (r *backendRunner) archive(ctx context.Context, msg *TaskMessage, err error) {
	msg.ErrorMsg = err.Error()
	msg.LastFailedAt = time.Now()
	if e := r.backend.Archive(ctx, msg, err.Error()); e != nil {
		r.logger.Errorf("Could not archive backend task id=%s: %v", msg.ID, e)
	}
}

func (r *backendRunner) perform(handler Handler, ctx context.Context, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			r.logger.Errorf("recovering from panic. See the stack trace below for details:\n%s", string(debug.Stack()))
			err = fmt.Errorf("panic: %v", x)
		}
	}()
	return handler.ProcessTask(ctx, task)
}
