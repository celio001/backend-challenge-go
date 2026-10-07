package app

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.uber.org/fx"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/usecase/reconcile"
)

func TestModuleGraphIsComplete(t *testing.T) {
	if err := fx.ValidateApp(Module); err != nil {
		t.Fatal(err)
	}
}

func TestMissingConfigPreventsBoot(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("OIDC_ISSUER", "")
	if err := New().Err(); err == nil {
		t.Fatal("app was built without configuration")
	}
}

func TestDivergenceLoggerReportsAtErrorLevelWithTheIdentifiersToDiagnose(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	stored, _ := money.FromMinor(98000, money.BRL)
	calculated, _ := money.FromMinor(97500, money.BRL)
	difference, _ := money.FromMinor(500, money.BRL)

	newDivergenceLogger(log).Divergence(context.Background(), reconcile.Report{
		WalletID: "wallet-1", Stored: stored, Calculated: calculated, Difference: difference, CheckedEntries: 2,
	})

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not JSON: %q", buf.String())
	}
	want := map[string]any{"level": "ERROR", "walletId": "wallet-1", "stored": "980.00", "calculated": "975.00", "difference": "5.00", "currency": "BRL", "checkedEntries": float64(2)}
	for k, v := range want {
		if line[k] != v {
			t.Fatalf("log field %s = %v, want %v (line: %s)", k, line[k], v, buf.String())
		}
	}
}
