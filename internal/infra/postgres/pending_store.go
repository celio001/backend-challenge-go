package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/resolvereference"
)

// PendingStore reserves due PENDING_REFERENCE rows for the reference resolver. It works on the pool, outside any unit of work:
// the reservation must commit at once so other replicas see the lease.
type PendingStore struct {
	db *pgxpool.Pool
}

func NewPendingStore(db *pgxpool.Pool) *PendingStore {
	return &PendingStore{db: db}
}

func (s *PendingStore) Claim(ctx context.Context, limit int, lease time.Duration) ([]resolvereference.Pending, error) {
	rows, err := s.db.Query(ctx,
		`UPDATE wager_transactions w
		    SET locked_until = now() + make_interval(secs => $2)
		   FROM (SELECT id FROM wager_transactions
		          WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now() AND (locked_until IS NULL OR locked_until < now())
		          ORDER BY next_attempt_at
		          LIMIT $1
		          FOR UPDATE SKIP LOCKED) due
		  WHERE w.id = due.id
		 RETURNING w.id, w.wallet_id`,
		limit, lease.Seconds())
	if err != nil {
		return nil, fmt.Errorf("claim pending references: %w", err)
	}
	defer rows.Close()

	var pending []resolvereference.Pending
	for rows.Next() {
		var id, walletID string
		if err := rows.Scan(&id, &walletID); err != nil {
			return nil, fmt.Errorf("scan pending reference: %w", err)
		}
		pending = append(pending, resolvereference.Pending{ID: wallet.TxID(id), WalletID: wallet.WalletID(walletID)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending references: %w", err)
	}
	return pending, nil
}

func (s *PendingStore) Release(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if _, err := s.db.Exec(ctx,
		`UPDATE wager_transactions SET locked_until = NULL WHERE id = ANY($1::uuid[]) AND status = 'PENDING_REFERENCE'`, ids); err != nil {
		return fmt.Errorf("release pending leases: %w", err)
	}
	return nil
}
