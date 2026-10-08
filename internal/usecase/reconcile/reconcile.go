// Package reconcile checks a wallet balance against its ledger. It only reads: a divergence is reported, never repaired.
package reconcile

import (
	"context"
	"fmt"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/pkg/uuid"
)

// Snapshot is what the reader saw at one instant: the stored balance and the ledger's totals.
type Snapshot struct {
	Stored  money.Money
	Credits money.Money
	Debits  money.Money
	Entries int64
}

type Reader interface {
	// Snapshot reads balance and ledger from one consistent view; wallet.ErrNotFound if the wallet does not exist.
	Snapshot(ctx context.Context, id wallet.WalletID) (Snapshot, error)
}

type Report struct {
	WalletID   wallet.WalletID
	Stored     money.Money
	Calculated money.Money
	Difference     money.Money
	Consistent     bool
	CheckedEntries int64
}

// Observer is told about every divergence, so it can be logged and counted.
type Observer interface {
	Divergence(ctx context.Context, r Report)
}

type UseCase struct {
	reader   Reader
	observer Observer
}

func New(reader Reader, observer Observer) *UseCase {
	return &UseCase{reader: reader, observer: observer}
}

func (uc *UseCase) Execute(ctx context.Context, id string) (Report, error) {
	if !uuid.Valid(id) {
		return Report{}, fmt.Errorf("%w: wallet id must be a UUID", wallet.ErrInvalidID)
	}
	snap, err := uc.reader.Snapshot(ctx, wallet.WalletID(id))
	if err != nil {
		return Report{}, err
	}

	calculated, err := snap.Credits.Sub(snap.Debits)
	if err != nil {
		return Report{}, fmt.Errorf("sum ledger of wallet %s: %w", id, err)
	}
	difference, err := snap.Stored.Sub(calculated)
	if err != nil {
		return Report{}, fmt.Errorf("compare wallet %s with its ledger: %w", id, err)
	}
	report := Report{
		WalletID: wallet.WalletID(id), Stored: snap.Stored, Calculated: calculated, Difference: difference,
		Consistent: difference.IsZero(), CheckedEntries: snap.Entries,
	}
	if !report.Consistent {
		uc.observer.Divergence(ctx, report)
	}
	return report, nil
}
