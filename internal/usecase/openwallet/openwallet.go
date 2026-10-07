package openwallet

import (
	"context"
	"fmt"

	"github.com/celio001/backend-challenge-go/internal/domain/event"
	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/pkg/uuid"
)

type Input struct {
	PlayerID       string
	InitialBalance money.Money
	CorrelationID  string
}

type UseCase struct {
	uow   usecase.UnitOfWork
	clock usecase.Clock
	ids   usecase.IDGenerator
}

func New(uow usecase.UnitOfWork, clock usecase.Clock, ids usecase.IDGenerator) *UseCase {
	return &UseCase{uow: uow, clock: clock, ids: ids}
}

func (uc *UseCase) Execute(ctx context.Context, in Input) (*wallet.Wallet, error) {
	if !uuid.Valid(in.PlayerID) {
		return nil, fmt.Errorf("%w: player id must be a UUID", wallet.ErrInvalidID)
	}
	if !in.InitialBalance.IsValid() {
		return nil, money.ErrUninitialized
	}
	if in.InitialBalance.IsNegative() {
		return nil, money.ErrNegative
	}

	now := uc.clock.Now()
	walletID := wallet.WalletID(uc.ids.NewID())
	playerID := wallet.PlayerID(in.PlayerID)

	if in.InitialBalance.IsZero() {
		w, err := wallet.Open(walletID, playerID, in.InitialBalance.Currency(), now)
		if err != nil {
			return nil, err
		}
		err = uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
			return createWallet(ctx, tx, w)
		})
		if err != nil {
			return nil, err
		}
		return w, nil
	}

	openingID := wallet.TxID(uc.ids.NewID())
	w, entry, err := wallet.OpenWithBalance(walletID, playerID, in.InitialBalance, openingID, now)
	if err != nil {
		return nil, err
	}
	opening, err := wager.NewOpening(openingID, walletID, playerID, in.InitialBalance, in.CorrelationID, now)
	if err != nil {
		return nil, err
	}
	processed, err := event.NewWagerTransactionProcessed(uc.ids.NewID(), "", opening)
	if err != nil {
		return nil, err
	}
	changed, err := event.NewWalletBalanceChanged(uc.ids.NewID(), in.CorrelationID, "", entry)
	if err != nil {
		return nil, err
	}

	err = uc.uow.Do(ctx, func(ctx context.Context, tx usecase.Repos) error {
		if err := createWallet(ctx, tx, w); err != nil {
			return err
		}
		if err := tx.Transactions().Insert(ctx, opening); err != nil {
			return fmt.Errorf("insert opening transaction: %w", err)
		}
		if err := tx.Ledger().Append(ctx, entry); err != nil {
			return fmt.Errorf("append opening ledger entry: %w", err)
		}
		return usecase.AddEvents(ctx, tx, processed, changed)
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func createWallet(ctx context.Context, tx usecase.Repos, w *wallet.Wallet) error {
	if err := tx.Wallets().Create(ctx, w); err != nil {
		return fmt.Errorf("create wallet: %w", err)
	}
	return nil
}
