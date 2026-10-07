package queries

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
)

const walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"

type fakeWallets struct {
	w   *wallet.Wallet
	err error
}

func (f fakeWallets) ByID(context.Context, wallet.WalletID) (*wallet.Wallet, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.w, nil
}

type fakeLedger struct {
	items []LedgerItem
	err   error
}

func (f fakeLedger) After(_ context.Context, _ wallet.WalletID, after int64, limit int) ([]LedgerItem, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []LedgerItem
	for _, it := range f.items {
		if it.WalletVersion > after && len(out) < limit {
			out = append(out, it)
		}
	}
	return out, nil
}

func ledgerOf(n int) []LedgerItem {
	items := make([]LedgerItem, n)
	for i := range items {
		items[i] = LedgerItem{WalletVersion: int64(i + 1)}
	}
	return items
}

func versions(items []LedgerItem) []int64 {
	out := make([]int64, len(items))
	for i, it := range items {
		out[i] = it.WalletVersion
	}
	return out
}

func TestWallet(t *testing.T) {
	zero, _ := money.Zero(money.BRL)
	now := time.Now()
	w, err := wallet.Rehydrate(walletID, "p1", zero, 1, now, now)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		id      string
		readErr error
		wantErr error
	}{
		{name: "found", id: walletID},
		{name: "not found", id: walletID, readErr: wallet.ErrNotFound, wantErr: wallet.ErrNotFound},
		{name: "id is not a uuid", id: "w1", wantErr: wallet.ErrInvalidID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(fakeWallets{w: w, err: tt.readErr}, fakeLedger{}).Wallet(context.Background(), tt.id)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got != w {
				t.Fatalf("wallet = %v", got)
			}
		})
	}
}

func TestLedger(t *testing.T) {
	errDB := errors.New("db down")

	tests := []struct {
		name       string
		items      []LedgerItem
		cursor     string
		limit      int
		walletErr  error
		ledgerErr  error
		wantErr    error
		want       []int64
		wantCursor int64
	}{
		{name: "empty ledger", limit: 2},
		{name: "first page has next cursor", items: ledgerOf(5), limit: 2, want: []int64{1, 2}, wantCursor: 2},
		{name: "exact fit has no next cursor", items: ledgerOf(2), limit: 2, want: []int64{1, 2}},
		{name: "default limit", items: ledgerOf(3), want: []int64{1, 2, 3}},
		{name: "second page", items: ledgerOf(5), cursor: encodeCursor(2), limit: 2, want: []int64{3, 4}, wantCursor: 4},
		{name: "last page", items: ledgerOf(5), cursor: encodeCursor(4), limit: 2, want: []int64{5}},
		{name: "cursor is not base64", items: ledgerOf(1), cursor: "***", limit: 2, wantErr: ErrInvalidCursor},
		{name: "cursor is not json", items: ledgerOf(1), cursor: "bm90LWpzb24", limit: 2, wantErr: ErrInvalidCursor},
		{name: "cursor version below one", items: ledgerOf(1), cursor: encodeCursor(0), limit: 2, wantErr: ErrInvalidCursor},
		{name: "negative limit", items: ledgerOf(1), limit: -1, wantErr: ErrInvalidLimit},
		{name: "limit above max", items: ledgerOf(1), limit: MaxLimit + 1, wantErr: ErrInvalidLimit},
		{name: "wallet not found", walletErr: wallet.ErrNotFound, limit: 2, wantErr: wallet.ErrNotFound},
		{name: "ledger failure", ledgerErr: errDB, limit: 2, wantErr: errDB},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zero, _ := money.Zero(money.BRL)
			now := time.Now()
			w, _ := wallet.Rehydrate(walletID, "p1", zero, 1, now, now)
			q := New(fakeWallets{w: w, err: tt.walletErr}, fakeLedger{items: tt.items, err: tt.ledgerErr})

			page, err := q.Ledger(context.Background(), walletID, tt.cursor, tt.limit)

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			got := versions(page.Items)
			if len(got) != len(tt.want) {
				t.Fatalf("versions = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("versions = %v, want %v", got, tt.want)
				}
			}
			wantCursor := ""
			if tt.wantCursor != 0 {
				wantCursor = encodeCursor(tt.wantCursor)
			}
			if page.NextCursor != wantCursor {
				t.Fatalf("nextCursor = %q, want %q", page.NextCursor, wantCursor)
			}
		})
	}
}

func TestCursorMatchesDocumentedFormat(t *testing.T) {
	if got := encodeCursor(1); got != "eyJ2IjoxfQ" {
		t.Fatalf("encodeCursor(1) = %q", got)
	}
}
