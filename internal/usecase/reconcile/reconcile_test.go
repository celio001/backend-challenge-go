package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

const walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"

type fakeReader struct {
	snap  Snapshot
	err   error
	calls int
}

func (f *fakeReader) Snapshot(context.Context, wallet.WalletID) (Snapshot, error) {
	f.calls++
	return f.snap, f.err
}

type fakeObserver struct{ got []Report }

func (f *fakeObserver) Divergence(_ context.Context, r Report) { f.got = append(f.got, r) }

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestExecute(t *testing.T) {
	tests := []struct {
		name           string
		id             string
		stored         int64
		credits        int64
		debits         int64
		entries        int64
		readerErr      error
		wantErr        error
		wantReads      int
		wantConsistent bool
		wantCalculated int64
		wantDifference int64
		wantAlerts     int
	}{
		{name: "balance equals credits minus debits", id: walletID, stored: 97500, credits: 100000, debits: 2500, entries: 2, wantReads: 1, wantConsistent: true, wantCalculated: 97500},
		{name: "an empty wallet with no ledger is consistent", id: walletID, wantReads: 1, wantConsistent: true},
		{name: "balance above the ledger", id: walletID, stored: 98000, credits: 100000, debits: 2500, entries: 2, wantReads: 1, wantCalculated: 97500, wantDifference: 500, wantAlerts: 1},
		{name: "balance below the ledger gives a negative difference", id: walletID, stored: 97000, credits: 100000, debits: 2500, entries: 2, wantReads: 1, wantCalculated: 97500, wantDifference: -500, wantAlerts: 1},
		{name: "debits above credits make the calculated balance negative", id: walletID, stored: 0, credits: 100, debits: 300, entries: 2, wantReads: 1, wantCalculated: -200, wantDifference: 200, wantAlerts: 1},
		{name: "an id that is not a UUID never reaches the database", id: "nope", wantErr: wallet.ErrInvalidID},
		{name: "unknown wallet", id: walletID, readerErr: wallet.ErrNotFound, wantErr: wallet.ErrNotFound, wantReads: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := &fakeReader{err: tt.readerErr, snap: Snapshot{Stored: brl(t, tt.stored), Credits: brl(t, tt.credits), Debits: brl(t, tt.debits), Entries: tt.entries}}
			obs := &fakeObserver{}

			got, err := New(reader, obs).Execute(context.Background(), tt.id)

			if !errors.Is(err, tt.wantErr) || reader.calls != tt.wantReads {
				t.Fatalf("err = %v (want %v), reads = %d (want %d)", err, tt.wantErr, reader.calls, tt.wantReads)
			}
			if tt.wantErr != nil {
				if len(obs.got) != 0 {
					t.Fatal("an error must not be reported as a divergence")
				}
				return
			}
			if got.Consistent != tt.wantConsistent || got.Calculated.Minor() != tt.wantCalculated || got.Difference.Minor() != tt.wantDifference ||
				got.Stored.Minor() != tt.stored || got.CheckedEntries != tt.entries || string(got.WalletID) != tt.id {
				t.Fatalf("report = %+v", got)
			}
			if len(obs.got) != tt.wantAlerts {
				t.Fatalf("divergence alerts = %d, want %d", len(obs.got), tt.wantAlerts)
			}
			if tt.wantAlerts == 1 && obs.got[0] != got {
				t.Fatalf("the alert carries %+v, the response %+v", obs.got[0], got)
			}
		})
	}

	t.Run("mixed currencies in the totals are an error, not a verdict", func(t *testing.T) {
		usd, _ := money.FromMinor(100, money.Currency("USD"))
		reader := &fakeReader{snap: Snapshot{Stored: brl(t, 100), Credits: usd, Debits: brl(t, 0)}}
		obs := &fakeObserver{}
		if _, err := New(reader, obs).Execute(context.Background(), walletID); !errors.Is(err, money.ErrCurrencyMismatch) {
			t.Fatalf("err = %v", err)
		}
		if len(obs.got) != 0 {
			t.Fatal("alerted on an error")
		}
	})
}
