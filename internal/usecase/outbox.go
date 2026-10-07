package usecase

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/celio001/backend-challenge-go/internal/domain/event"
)

// AddEvents stores each event as an immutable snapshot of its envelope in the outbox of the current unit of work.
func AddEvents(ctx context.Context, tx Repos, events ...event.Event) error {
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal %s: %w", e.Type, err)
		}
		err = tx.Outbox().Add(ctx, OutboxEvent{
			ID:            e.ID,
			AggregateType: e.AggregateType,
			AggregateID:   e.AggregateID,
			PartitionKey:  e.PartitionKey,
			Type:          e.Type,
			Version:       e.Version,
			Payload:       payload,
			OccurredAt:    e.OccurredAt,
		})
		if err != nil {
			return fmt.Errorf("add %s event: %w", e.Type, err)
		}
	}
	return nil
}
