package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/celio001/backend-challenge-go/internal/auth"
	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/processwager"
)

const idempotencyHeader = "Idempotency-Key"

type wageringHandlers struct {
	process ProcessWager
	queries TransactionQueries
	log     *slog.Logger
}

type wagerRequest struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
}

type wagerResponse struct {
	TransactionID    string       `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          *money.Money `json:"balance,omitempty"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

type transactionResponse struct {
	TransactionID                  string       `json:"transactionId"`
	Status                         string       `json:"status"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	WalletID                       string       `json:"walletId"`
	PlayerID                       string       `json:"playerId"`
	RoundID                        string       `json:"roundId,omitempty"`
	GameID                         string       `json:"gameId,omitempty"`
	Kind                           string       `json:"kind"`
	Money                          money.Money  `json:"money"`
	Balance                        *money.Money `json:"balance,omitempty"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         string       `json:"referenceTransactionId,omitempty"`
	ExpiresAt                      string       `json:"expiresAt,omitempty"`
	CreatedAt                      string       `json:"createdAt"`
	UpdatedAt                      string       `json:"updatedAt"`
}

func (h wageringHandlers) post(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.PrincipalFrom(r.Context())

	key := r.Header.Get(idempotencyHeader)
	if key == "" {
		writeProblem(w, errMissingKey)
		return
	}

	var req wagerRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(h.log, w, r, err)
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeProblem(w, errInvalidRequest.with("unexpected data after the JSON body"))
		return
	}

	// The provider is whoever authenticated; the body can only repeat it, never choose it.
	if principal.ProviderID == "" || req.ProviderID != principal.ProviderID {
		writeProblem(w, errProviderMismatch)
		return
	}

	out, err := h.process.Execute(r.Context(), processwager.Input{
		ProviderID:                     principal.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		IdempotencyKey:                 key,
		PlayerID:                       req.PlayerID,
		WalletID:                       req.WalletID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Money:                          req.Money,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID:                  correlationIDFrom(r.Context()),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	resp := wagerResponse{TransactionID: out.TransactionID, Status: string(out.Status), FailureCode: string(out.FailureCode), IdempotentReplay: out.Replay}
	status := http.StatusOK
	switch out.Status {
	case wager.StatusProcessed:
		resp.Balance = &out.Balance
	case wager.StatusPendingReference:
		status = http.StatusAccepted
	case wager.StatusRejected:
		status = http.StatusUnprocessableEntity
	default:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, resp)
}

func (h wageringHandlers) get(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.PrincipalFrom(r.Context())
	t, err := h.queries.Transaction(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	// Someone else's transaction is indistinguishable from a missing one.
	if !principal.HasRole(auth.RoleWalletAdmin) && t.ProviderID() != principal.ProviderID {
		writeProblem(w, errTxNotFound)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}

func (h wageringHandlers) getByProvider(w http.ResponseWriter, r *http.Request) {
	principal, _ := auth.PrincipalFrom(r.Context())
	if principal.ProviderID == "" || r.PathValue("provider") != principal.ProviderID {
		writeProblem(w, errForbidden)
		return
	}
	t, err := h.queries.ProviderTransaction(r.Context(), principal.ProviderID, r.PathValue("external"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}

// writeError applies the wagering-specific mapping, where a missing wallet is a correctable 422 rather than a 404.
func (h wageringHandlers) writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, processwager.ErrMissingIdempotencyKey):
		writeProblem(w, errMissingKey)
	case errors.Is(err, processwager.ErrKindNotAllowed):
		writeProblem(w, errKindNotAllowed)
	case errors.Is(err, processwager.ErrValidation):
		writeProblem(w, errValidation.with(err.Error()))
	case errors.Is(err, processwager.ErrIdempotencyKeyReused):
		writeProblem(w, errKeyReused)
	case errors.Is(err, processwager.ErrExternalIDConflict):
		writeProblem(w, errExternalConflict)
	case errors.Is(err, processwager.ErrPlayerWalletMismatch):
		writeProblem(w, errPlayerMismatch)
	case errors.Is(err, wallet.ErrNotFound):
		writeProblem(w, errWagerWallet)
	case errors.Is(err, wager.ErrNotFound):
		writeProblem(w, errTxNotFound)
	default:
		writeError(h.log, w, r, err)
	}
}

func transactionView(t *wager.Transaction) transactionResponse {
	resp := transactionResponse{
		TransactionID: string(t.ID()), Status: string(t.Status()), ProviderID: t.ProviderID(),
		ExternalTransactionID: t.ExternalTransactionID(), WalletID: string(t.WalletID()), PlayerID: string(t.PlayerID()),
		RoundID: t.RoundID(), GameID: t.GameID(), Kind: string(t.Kind()), Money: t.Amount(),
		FailureCode: string(t.FailureCode()), ReferenceExternalTransactionID: t.ReferenceExternalID(),
		ReferenceTransactionID: string(t.ReferenceTxID()),
		CreatedAt:              t.CreatedAt().UTC().Format(timeLayout), UpdatedAt: t.UpdatedAt().UTC().Format(timeLayout),
	}
	if t.Status() == wager.StatusProcessed {
		balance := t.ResultBalance()
		resp.Balance = &balance
	}
	if !t.ExpiresAt().IsZero() {
		resp.ExpiresAt = t.ExpiresAt().UTC().Format(timeLayout)
	}
	return resp
}
