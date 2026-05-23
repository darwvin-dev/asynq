// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

// Package postgres provides a PostgreSQL-backed implementation of asynq.Backend.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hibiken/asynq"
)

const schema = `
CREATE TABLE IF NOT EXISTS asynq_jobs (
	id TEXT PRIMARY KEY,
	type TEXT NOT NULL,
	payload BYTEA NOT NULL,
	headers JSONB NOT NULL DEFAULT '{}',
	queue TEXT NOT NULL,
	retry INTEGER NOT NULL,
	retried INTEGER NOT NULL DEFAULT 0,
	error_msg TEXT NOT NULL DEFAULT '',
	last_failed_at TIMESTAMPTZ,
	timeout_seconds BIGINT NOT NULL DEFAULT 0,
	deadline TIMESTAMPTZ,
	unique_key TEXT NOT NULL DEFAULT '',
	group_key TEXT NOT NULL DEFAULT '',
	retention_seconds BIGINT NOT NULL DEFAULT 0,
	completed_at TIMESTAMPTZ,
	run_at TIMESTAMPTZ NOT NULL,
	state TEXT NOT NULL,
	locked_until TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS asynq_jobs_dequeue_idx ON asynq_jobs (queue, state, run_at, locked_until);
`

// Backend stores tasks in PostgreSQL.
type Backend struct {
	db *sql.DB
}

// NewBackend returns a PostgreSQL-backed backend.
func NewBackend(db *sql.DB) *Backend {
	return &Backend{db: db}
}

// Migrate creates the database objects required by the backend.
func (b *Backend) Migrate(ctx context.Context) error {
	_, err := b.db.ExecContext(ctx, schema)
	return err
}

// Ping verifies the database connection.
func (b *Backend) Ping(ctx context.Context) error {
	return b.db.PingContext(ctx)
}

// Close closes the database handle.
func (b *Backend) Close() error {
	return b.db.Close()
}

// Enqueue inserts msg into the jobs table.
func (b *Backend) Enqueue(ctx context.Context, msg *asynq.TaskMessage) error {
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return fmt.Errorf("postgres backend: marshal headers: %w", err)
	}
	state := "pending"
	runAt := msg.RunAt
	if runAt.IsZero() {
		runAt = time.Now()
	}
	if runAt.After(time.Now()) {
		state = "scheduled"
	}
	_, err = b.db.ExecContext(ctx, `
INSERT INTO asynq_jobs (
	id, type, payload, headers, queue, retry, retried, error_msg, last_failed_at,
	timeout_seconds, deadline, unique_key, group_key, retention_seconds,
	completed_at, run_at, state
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (id) DO NOTHING`,
		msg.ID, msg.Type, msg.Payload, headers, msg.Queue, msg.Retry, msg.Retried,
		msg.ErrorMsg, nullableTime(msg.LastFailedAt), int64(msg.Timeout.Seconds()),
		nullableTime(msg.Deadline), msg.UniqueKey, msg.GroupKey, int64(msg.Retention.Seconds()),
		nullableTime(msg.CompletedAt), runAt, state)
	return err
}

// Dequeue locks and returns the next processable job.
func (b *Backend) Dequeue(ctx context.Context, queues []string) (*asynq.TaskMessage, error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	where, args := queuePredicate(queues)
	args = append(args, time.Now())
	nowPos := len(args)
	query := fmt.Sprintf(`
SELECT id, type, payload, headers, queue, retry, retried, error_msg, last_failed_at,
	timeout_seconds, deadline, unique_key, group_key, retention_seconds,
	completed_at, run_at
FROM asynq_jobs
WHERE %s
  AND state IN ('pending', 'scheduled', 'retry')
  AND run_at <= $%d
  AND (locked_until IS NULL OR locked_until <= $%d)
ORDER BY run_at ASC, created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED`, where, nowPos, nowPos)

	msg, err := scanMessage(tx.QueryRowContext(ctx, query, args...))
	if err == sql.ErrNoRows {
		return nil, asynq.ErrNoProcessableTask
	}
	if err != nil {
		return nil, err
	}
	lockedUntil := time.Now().Add(lockDuration(msg))
	if _, err := tx.ExecContext(ctx, `UPDATE asynq_jobs SET state='active', locked_until=$1, updated_at=NOW() WHERE id=$2`, lockedUntil, msg.ID); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return msg, nil
}

// Done removes a successfully processed job.
func (b *Backend) Done(ctx context.Context, msg *asynq.TaskMessage) error {
	_, err := b.db.ExecContext(ctx, `DELETE FROM asynq_jobs WHERE id=$1`, msg.ID)
	return err
}

// Retry schedules msg for a future retry.
func (b *Backend) Retry(ctx context.Context, msg *asynq.TaskMessage, processAt time.Time, errMsg string) error {
	_, err := b.db.ExecContext(ctx, `
UPDATE asynq_jobs
SET state='retry', retried=$1, error_msg=$2, last_failed_at=$3, run_at=$4, locked_until=NULL, updated_at=NOW()
WHERE id=$5`, msg.Retried, errMsg, time.Now(), processAt, msg.ID)
	return err
}

// Archive moves msg to the archived state.
func (b *Backend) Archive(ctx context.Context, msg *asynq.TaskMessage, errMsg string) error {
	_, err := b.db.ExecContext(ctx, `
UPDATE asynq_jobs
SET state='archived', error_msg=$1, last_failed_at=$2, locked_until=NULL, updated_at=NOW()
WHERE id=$3`, errMsg, time.Now(), msg.ID)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMessage(row scanner) (*asynq.TaskMessage, error) {
	var msg asynq.TaskMessage
	var headers []byte
	var lastFailedAt, deadline, completedAt sql.NullTime
	var timeoutSeconds, retentionSeconds int64
	if err := row.Scan(
		&msg.ID, &msg.Type, &msg.Payload, &headers, &msg.Queue, &msg.Retry, &msg.Retried,
		&msg.ErrorMsg, &lastFailedAt, &timeoutSeconds, &deadline, &msg.UniqueKey,
		&msg.GroupKey, &retentionSeconds, &completedAt, &msg.RunAt,
	); err != nil {
		return nil, err
	}
	if len(headers) > 0 {
		if err := json.Unmarshal(headers, &msg.Headers); err != nil {
			return nil, fmt.Errorf("postgres backend: unmarshal headers: %w", err)
		}
	}
	msg.LastFailedAt = timeFromNull(lastFailedAt)
	msg.Timeout = time.Duration(timeoutSeconds) * time.Second
	msg.Deadline = timeFromNull(deadline)
	msg.Retention = time.Duration(retentionSeconds) * time.Second
	msg.CompletedAt = timeFromNull(completedAt)
	return &msg, nil
}

func queuePredicate(queues []string) (string, []any) {
	if len(queues) == 0 {
		return "queue = $1", []any{"default"}
	}
	parts := make([]string, len(queues))
	args := make([]any, len(queues))
	for i, q := range queues {
		parts[i] = fmt.Sprintf("$%d", i+1)
		args[i] = q
	}
	return "queue IN (" + strings.Join(parts, ",") + ")", args
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func timeFromNull(t sql.NullTime) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time
}

func lockDuration(msg *asynq.TaskMessage) time.Duration {
	if msg.Timeout > 0 {
		return msg.Timeout
	}
	return 30 * time.Minute
}
