package app

import (
	"testing"

	"go.uber.org/fx"
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
