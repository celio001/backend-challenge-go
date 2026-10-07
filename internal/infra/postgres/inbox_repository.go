package postgres

import (
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/celio001/backend-challenge-go/internal/usecase"
)

type inboxDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type InboxRepository struct {
	db inboxDB
}

func NewInboxRepository(db inboxDB) *InboxRepository {
	return &InboxRepository{db: db}
}

// The row is inserted already completed: it shares the transaction of the operation, so it exists only if that commits.
func (r *InboxRepository) Register(ctx context.Context, m usecase.InboxMessage) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
		 VALUES ($1, $2, $3, $4, $4)
		 ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		m.Consumer, m.MessageID, m.Hash, m.ReceivedAt)
	if err != nil {
		return false, fmt.Errorf("insert inbox message: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return false, nil
	}

	var stored []byte
	if err := r.db.QueryRow(ctx,
		`SELECT payload_hash FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
		m.Consumer, m.MessageID).Scan(&stored); err != nil {
		return false, fmt.Errorf("read inbox message: %w", err)
	}
	if !bytes.Equal(stored, m.Hash) {
		return false, usecase.ErrInboxHashMismatch
	}
	return true, nil
}
