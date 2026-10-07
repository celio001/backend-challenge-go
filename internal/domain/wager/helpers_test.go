package wager

import (
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func hash() []byte { return make([]byte, payloadHashSize) }

func input(t *testing.T, kind Kind, minor int64) ExternalInput {
	t.Helper()
	in := ExternalInput{
		ID:                    "tx1",
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-1",
		IdempotencyKey:        "provider-a:ext-1",
		PayloadHash:           hash(),
		WalletID:              wallet.WalletID("w1"),
		PlayerID:              wallet.PlayerID("p1"),
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  kind,
		Amount:                brl(t, minor),
	}
	if kind == KindRefund || kind == KindRollback {
		in.ReferenceExternalID = "ext-0"
	}
	return in
}

func mustTx(t *testing.T, in ExternalInput) *Transaction {
	t.Helper()
	tx, err := NewExternal(in, t0)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func newTx(t *testing.T, kind Kind, minor int64) *Transaction {
	t.Helper()
	tx, err := NewExternal(input(t, kind, minor), t0)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}
