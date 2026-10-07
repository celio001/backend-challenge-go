package httpapi

import (
	"encoding/json"
	"net/http"
)

const problemBase = "https://wallet.local/problems/"

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

var (
	errUnauthenticated  = problem{Type: problemBase + "unauthenticated", Title: "Authentication required", Status: http.StatusUnauthorized, Code: "UNAUTHENTICATED"}
	errForbidden        = problem{Type: problemBase + "forbidden", Title: "Not allowed", Status: http.StatusForbidden, Code: "FORBIDDEN"}
	errInvalidRequest   = problem{Type: problemBase + "invalid-request", Title: "Invalid request", Status: http.StatusBadRequest, Code: "INVALID_REQUEST"}
	errInvalidMoney     = problem{Type: problemBase + "invalid-money", Title: "Invalid monetary value", Status: http.StatusBadRequest, Code: "INVALID_MONEY"}
	errInvalidID        = problem{Type: problemBase + "invalid-id", Title: "Invalid identifier", Status: http.StatusBadRequest, Code: "INVALID_ID"}
	errInvalidCursor    = problem{Type: problemBase + "invalid-cursor", Title: "Invalid cursor", Status: http.StatusBadRequest, Code: "INVALID_CURSOR"}
	errInvalidLimit     = problem{Type: problemBase + "invalid-limit", Title: "Invalid limit", Status: http.StatusBadRequest, Code: "INVALID_LIMIT"}
	errWalletNotFound   = problem{Type: problemBase + "wallet-not-found", Title: "Wallet not found", Status: http.StatusNotFound, Code: "WALLET_NOT_FOUND"}
	errWalletExists     = problem{Type: problemBase + "wallet-already-exists", Title: "Wallet already exists for this player and currency", Status: http.StatusConflict, Code: "WALLET_ALREADY_EXISTS"}
	errValidation       = problem{Type: problemBase + "validation-error", Title: "Invalid request", Status: http.StatusBadRequest, Code: "VALIDATION_ERROR"}
	errMissingKey       = problem{Type: problemBase + "missing-idempotency-key", Title: "Idempotency-Key header is required", Status: http.StatusBadRequest, Code: "MISSING_IDEMPOTENCY_KEY"}
	errKindNotAllowed   = problem{Type: problemBase + "kind-not-allowed", Title: "Transaction kind not allowed", Status: http.StatusBadRequest, Code: "KIND_NOT_ALLOWED"}
	errProviderMismatch = problem{Type: problemBase + "provider-mismatch", Title: "Provider does not match the authenticated identity", Status: http.StatusForbidden, Code: "FORBIDDEN"}
	errWagerWallet      = problem{Type: problemBase + "wallet-not-found", Title: "Wallet not found", Status: http.StatusUnprocessableEntity, Code: "WALLET_NOT_FOUND"}
	errPlayerMismatch   = problem{Type: problemBase + "player-wallet-mismatch", Title: "Wallet belongs to another player", Status: http.StatusUnprocessableEntity, Code: "PLAYER_WALLET_MISMATCH"}
	errKeyReused        = problem{Type: problemBase + "idempotency-key-reused", Title: "Idempotency key reused with different payload", Status: http.StatusConflict, Code: "IDEMPOTENCY_KEY_REUSED"}
	errExternalConflict = problem{Type: problemBase + "external-transaction-id-conflict", Title: "External transaction id already used with another idempotency key", Status: http.StatusConflict, Code: "EXTERNAL_TRANSACTION_ID_CONFLICT"}
	errTxNotFound       = problem{Type: problemBase + "transaction-not-found", Title: "Transaction not found", Status: http.StatusNotFound, Code: "TRANSACTION_NOT_FOUND"}
	errUnavailable      = problem{Type: problemBase + "temporarily-unavailable", Title: "Temporarily unavailable", Status: http.StatusServiceUnavailable, Code: "TEMPORARILY_UNAVAILABLE"}
	errInternal         = problem{Type: problemBase + "internal-error", Title: "Internal error", Status: http.StatusInternalServerError, Code: "INTERNAL_ERROR"}
)

func (p problem) with(detail string) problem {
	p.Detail = detail
	return p
}

func writeProblem(w http.ResponseWriter, p problem) {
	if p.Status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
