package postgres

import (
	"context"
	"fmt"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

type OutboxWriter struct {
	db execer
}

func NewOutboxWriter(db execer) *OutboxWriter {
	return &OutboxWriter{db: db}
}

func (w *OutboxWriter) Add(ctx context.Context, e usecase.OutboxEvent) error {
	_, err := w.db.Exec(ctx,
		`INSERT INTO outbox_events (id, aggregate_type, aggregate_id, partition_key, event_type, event_version, payload, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		e.ID, e.AggregateType, e.AggregateID, e.PartitionKey, e.Type, e.Version, e.Payload, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
