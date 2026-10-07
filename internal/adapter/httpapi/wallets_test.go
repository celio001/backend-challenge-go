package httpapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

func TestReconciliation(t *testing.T) {
	report := func(stored, calculated, diff int64, entries int64) reconcile.Report {
		return reconcile.Report{
			WalletID: walletID, Stored: brlMinor(t, stored), Calculated: brlMinor(t, calculated), Difference: brlMinor(t, diff),
			Consistent: diff == 0, CheckedEntries: entries,
		}
	}
	tests := []struct {
		name       string
		token      string
		path       string
		report     reconcile.Report
		err        error
		wantStatus int
		wantCode   string
		wantBody   string
		wantCalls  int
	}{
		{
			name: "consistent wallet", token: "admin", path: "/wallets/" + walletID + "/reconciliation", report: report(97500, 97500, 0, 2), wantStatus: 200, wantCalls: 1,
			wantBody: `{"walletId":"` + walletID + `","storedBalance":{"amount":"975.00","currency":"BRL"},"calculatedBalance":{"amount":"975.00","currency":"BRL"},"difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}`,
		},
		{
			name: "a divergence is still a 200 and carries the signed difference", token: "admin", path: "/wallets/" + walletID + "/reconciliation", report: report(98000, 97500, 500, 2), wantStatus: 200, wantCalls: 1,
			wantBody: `{"walletId":"` + walletID + `","storedBalance":{"amount":"980.00","currency":"BRL"},"calculatedBalance":{"amount":"975.00","currency":"BRL"},"difference":{"amount":"5.00","currency":"BRL"},"consistent":false,"checkedEntries":2}`,
		},
		{
			name: "a ledger above the balance gives a negative difference", token: "admin", path: "/wallets/" + walletID + "/reconciliation", report: report(97000, 97500, -500, 2), wantStatus: 200, wantCalls: 1,
			wantBody: `{"walletId":"` + walletID + `","storedBalance":{"amount":"970.00","currency":"BRL"},"calculatedBalance":{"amount":"975.00","currency":"BRL"},"difference":{"amount":"-5.00","currency":"BRL"},"consistent":false,"checkedEntries":2}`,
		},
		{name: "unknown wallet", token: "admin", path: "/wallets/" + walletID + "/reconciliation", err: wallet.ErrNotFound, wantStatus: 404, wantCode: "WALLET_NOT_FOUND", wantCalls: 1},
		{name: "invalid id", token: "admin", path: "/wallets/nope/reconciliation", err: wallet.ErrInvalidID, wantStatus: 400, wantCode: "INVALID_ID", wantCalls: 1},
		{name: "providers cannot reconcile", token: "provider", path: "/wallets/" + walletID + "/reconciliation", wantStatus: 403, wantCode: "FORBIDDEN"},
		{name: "no token", path: "/wallets/" + walletID + "/reconciliation", wantStatus: 401, wantCode: "UNAUTHENTICATED"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t)
			e.reconcile.report, e.reconcile.err = tt.report, tt.err

			rec := do(e.h, http.MethodPost, tt.path, tt.token, "")

			if rec.Code != tt.wantStatus || e.reconcile.calls != tt.wantCalls {
				t.Fatalf("status = %d (want %d), calls = %d (want %d), body = %s", rec.Code, tt.wantStatus, e.reconcile.calls, tt.wantCalls, rec.Body.String())
			}
			if tt.wantCode != "" {
				var p map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p["code"] != tt.wantCode {
					t.Fatalf("body = %s", rec.Body.String())
				}
			}
			if tt.wantBody != "" && strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Fatalf("body = %s\nwant   %s", rec.Body.String(), tt.wantBody)
			}
		})
	}
}
