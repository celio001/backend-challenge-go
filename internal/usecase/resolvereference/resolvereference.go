// Package resolvereference retries operations that were parked as PENDING_REFERENCE, with the same rules as a new operation.
package resolvereference

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

type Pending struct {
	ID       wallet.TxID
	WalletID wallet.WalletID
}

type UseCase struct {
	uow     usecase.UnitOfWork
	clock   usecase.Clock
	process *processwager.UseCase
}

func New(uow usecase.UnitOfWork, clock usecase.Clock, process *processwager.UseCase) *UseCase {
	return &UseCase{uow: uow, clock: clock, process: process}
}

// Resolve returns the status the transaction has afterwards, or "" when another replica already settled it.
// Wallet first, then the pending row: the order every other writer of the wallet follows.
func (uc *UseCase) Resolve(ctx context.Context, p Pending) (wager.Status, error) {
	var status wager.Status
	err := uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
		status = ""
		w, err := tx.Wallets().Lock(ctx, p.WalletID)
		if err != nil {
			return err
		}
		t, err := tx.Transactions().LockPending(ctx, p.ID)
		if errors.Is(err, wager.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		out, err := uc.process.Resume(ctx, tx, t, w)
		if err != nil {
			return err
		}
		status = out.Status
		return nil
	})
	if errors.Is(err, usecase.ErrPermanent) {
		if failErr := uc.fail(ctx, p); failErr != nil {
			return "", fmt.Errorf("%w (recording the failure: %v)", err, failErr)
		}
		return wager.StatusFailed, err
	}
	if err != nil {
		return "", err
	}
	return status, nil
}

// fail audits a permanent failure in its own unit of work, since the one that hit it was rolled back; without it the row would be retried forever.
func (uc *UseCase) fail(ctx context.Context, p Pending) error {
	return uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
		t, err := tx.Transactions().LockPending(ctx, p.ID)
		if errors.Is(err, wager.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := t.Fail(uc.clock.Now()); err != nil {
			return err
		}
		return tx.Transactions().Update(ctx, t, time.Time{})
	})
}
