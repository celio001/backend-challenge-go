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
	errUnauthenticated = problem{Type: problemBase + "unauthenticated", Title: "Authentication required", Status: http.StatusUnauthorized, Code: "UNAUTHENTICATED"}
	errForbidden       = problem{Type: problemBase + "forbidden", Title: "Not allowed", Status: http.StatusForbidden, Code: "FORBIDDEN"}
	errInvalidRequest  = problem{Type: problemBase + "invalid-request", Title: "Invalid request", Status: http.StatusBadRequest, Code: "INVALID_REQUEST"}
	errInvalidMoney    = problem{Type: problemBase + "invalid-money", Title: "Invalid monetary value", Status: http.StatusBadRequest, Code: "INVALID_MONEY"}
	errInvalidID       = problem{Type: problemBase + "invalid-id", Title: "Invalid identifier", Status: http.StatusBadRequest, Code: "INVALID_ID"}
	errInvalidCursor   = problem{Type: problemBase + "invalid-cursor", Title: "Invalid cursor", Status: http.StatusBadRequest, Code: "INVALID_CURSOR"}
	errInvalidLimit    = problem{Type: problemBase + "invalid-limit", Title: "Invalid limit", Status: http.StatusBadRequest, Code: "INVALID_LIMIT"}
	errWalletNotFound  = problem{Type: problemBase + "wallet-not-found", Title: "Wallet not found", Status: http.StatusNotFound, Code: "WALLET_NOT_FOUND"}
	errWalletExists    = problem{Type: problemBase + "wallet-already-exists", Title: "Wallet already exists for this player and currency", Status: http.StatusConflict, Code: "WALLET_ALREADY_EXISTS"}
	errInternal        = problem{Type: problemBase + "internal-error", Title: "Internal error", Status: http.StatusInternalServerError, Code: "INTERNAL_ERROR"}
)

func (p problem) with(detail string) problem {
	p.Detail = detail
	return p
}

func writeProblem(w http.ResponseWriter, p problem) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
