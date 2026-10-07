package postgres

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/celio001/backend-challenge-go/internal/infra/outbox"
)

// OutboxStore leases outbox rows to relay workers. Every time comparison uses the database clock, so replicas with
// skewed clocks still agree on whose lease is valid.
type OutboxStore struct {
	db dbtx
}

func NewOutboxStore(db dbtx) *OutboxStore {
	return &OutboxStore{db: db}
}

func (s *OutboxStore) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]outbox.Message, error) {
	rows, err := s.db.Query(ctx,
		`WITH due AS (
		   SELECT id FROM outbox_events
		    WHERE published_at IS NULL AND next_attempt_at <= now() AND (locked_until IS NULL OR locked_until < now())
		    ORDER BY next_attempt_at, occurred_at
		    LIMIT $2
		    FOR UPDATE SKIP LOCKED)
		 UPDATE outbox_events o
		    SET locked_by = $1, locked_until = now() + make_interval(secs => $3)
		   FROM due
		  WHERE o.id = due.id
		 RETURNING o.id, o.partition_key, o.event_type, o.payload, o.attempts, o.occurred_at`,
		owner, limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	defer rows.Close()

	type claimed struct {
		msg        outbox.Message
		occurredAt time.Time
	}
	var got []claimed
	for rows.Next() {
		var c claimed
		if err := rows.Scan(&c.msg.ID, &c.msg.PartitionKey, &c.msg.Type, &c.msg.Payload, &c.msg.Attempts, &c.occurredAt); err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		got = append(got, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox events: %w", err)
	}

	// RETURNING has no order; send the oldest first so events of one wallet keep their sequence as far as possible.
	sort.SliceStable(got, func(i, j int) bool { return got[i].occurredAt.Before(got[j].occurredAt) })
	msgs := make([]outbox.Message, len(got))
	for i, c := range got {
		msgs[i] = c.msg
	}
	return msgs, nil
}

func (s *OutboxStore) MarkPublished(ctx context.Context, id, owner string) (bool, error) {
	tag, err := s.db.Exec(ctx,
		`UPDATE outbox_events
		    SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		  WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner)
	if err != nil {
		return false, fmt.Errorf("mark outbox event published: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *OutboxStore) MarkFailed(ctx context.Context, id, owner string, retryIn time.Duration, cause string) (bool, error) {
	tag, err := s.db.Exec(ctx,
		`UPDATE outbox_events
		    SET attempts = attempts + 1, next_attempt_at = now() + make_interval(secs => $3), last_error = $4,
		        locked_by = NULL, locked_until = NULL
		  WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, retryIn.Seconds(), truncate(cause, 1000))
	if err != nil {
		return false, fmt.Errorf("mark outbox event failed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *OutboxStore) Release(ctx context.Context, owner string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.db.Exec(ctx,
		`UPDATE outbox_events SET locked_by = NULL, locked_until = NULL
		  WHERE id = ANY($1::uuid[]) AND locked_by = $2 AND published_at IS NULL`, ids, owner); err != nil {
		return fmt.Errorf("release outbox leases: %w", err)
	}
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
