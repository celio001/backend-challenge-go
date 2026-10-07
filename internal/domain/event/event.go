package event

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

var (
	ErrMissingID       = errors.New("event: missing event id")
	ErrNotProcessed    = errors.New("event: transaction is not processed")
	ErrInvalidMovement = errors.New("event: ledger entry is not a balance movement")
)

const (
	TypeWagerTransactionProcessed = "WagerTransactionProcessed"
	TypeWalletBalanceChanged      = "WalletBalanceChanged"

	aggregateTransaction = "wager_transaction"
	aggregateWallet      = "wallet"
	timeLayout           = "2006-01-02T15:04:05.000Z"
)

// Event is the immutable envelope stored in the outbox; AggregateType and PartitionKey are routing data and are not serialized.
type Event struct {
	ID            string
	Type          string
	Version       int
	AggregateType string
	AggregateID   string
	PartitionKey  string
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
	Data          any
}

func (e Event) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		EventID       string `json:"eventId"`
		EventType     string `json:"eventType"`
		Version       int    `json:"version"`
		AggregateID   string `json:"aggregateId"`
		CorrelationID string `json:"correlationId"`
		CausationID   string `json:"causationId,omitempty"`
		OccurredAt    string `json:"occurredAt"`
		Data          any    `json:"data"`
	}{e.ID, e.Type, e.Version, e.AggregateID, e.CorrelationID, e.CausationID, e.OccurredAt.UTC().Format(timeLayout), e.Data})
}

type WagerTransactionProcessedData struct {
	TransactionID         string      `json:"transactionId"`
	WalletID              string      `json:"walletId"`
	PlayerID              string      `json:"playerId"`
	Kind                  string      `json:"kind"`
	Origin                string      `json:"origin"`
	Money                 money.Money `json:"money"`
	ResultBalance         money.Money `json:"resultBalance"`
	ProviderID            string      `json:"providerId,omitempty"`
	ExternalTransactionID string      `json:"externalTransactionId,omitempty"`
}

type WalletBalanceChangedData struct {
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

func NewWagerTransactionProcessed(id, causationID string, t *wager.Transaction) (Event, error) {
	if id == "" {
		return Event{}, ErrMissingID
	}
	if t.Status() != wager.StatusProcessed {
		return Event{}, ErrNotProcessed
	}
	return Event{
		ID:            id,
		Type:          TypeWagerTransactionProcessed,
		Version:       1,
		AggregateType: aggregateTransaction,
		AggregateID:   string(t.ID()),
		PartitionKey:  string(t.WalletID()),
		CorrelationID: t.CorrelationID(),
		CausationID:   causationID,
		OccurredAt:    t.UpdatedAt(),
		Data: WagerTransactionProcessedData{
			TransactionID:         string(t.ID()),
			WalletID:              string(t.WalletID()),
			PlayerID:              string(t.PlayerID()),
			Kind:                  string(t.Kind()),
			Origin:                string(t.Origin()),
			Money:                 t.Amount(),
			ResultBalance:         t.ResultBalance(),
			ProviderID:            t.ProviderID(),
			ExternalTransactionID: t.ExternalTransactionID(),
		},
	}, nil
}

func NewWalletBalanceChanged(id, correlationID, causationID string, e wallet.LedgerEntry) (Event, error) {
	if id == "" {
		return Event{}, ErrMissingID
	}
	if e.Direction() != wallet.Debit && e.Direction() != wallet.Credit {
		return Event{}, ErrInvalidMovement
	}
	return Event{
		ID:            id,
		Type:          TypeWalletBalanceChanged,
		Version:       1,
		AggregateType: aggregateWallet,
		AggregateID:   string(e.WalletID()),
		PartitionKey:  string(e.WalletID()),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    e.CreatedAt(),
		Data: WalletBalanceChangedData{
			WalletID:      string(e.WalletID()),
			TransactionID: string(e.TxID()),
			Direction:     string(e.Direction()),
			Money:         e.Amount(),
			BalanceBefore: e.BalanceBefore(),
			BalanceAfter:  e.BalanceAfter(),
			WalletVersion: e.WalletVersion(),
		},
	}, nil
}
