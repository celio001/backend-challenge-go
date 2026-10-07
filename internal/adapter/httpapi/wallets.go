package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/celio001/backend-challenge-go/internal/domain/money"
	"github.com/celio001/backend-challenge-go/internal/domain/wager"
	"github.com/celio001/backend-challenge-go/internal/domain/wallet"
	"github.com/celio001/backend-challenge-go/internal/usecase"
	"github.com/celio001/backend-challenge-go/internal/usecase/openwallet"
	"github.com/celio001/backend-challenge-go/internal/usecase/queries"
)

const (
	maxBodyBytes = 64 << 10
	timeLayout   = "2006-01-02T15:04:05.000Z"
)

type walletHandlers struct {
	open      OpenWallet
	queries   WalletQueries
	reconcile Reconciler
	log       *slog.Logger
}

type openWalletRequest struct {
	PlayerID       string      `json:"playerId"`
	InitialBalance money.Money `json:"initialBalance"`
}

type walletResponse struct {
	ID        string      `json:"id"`
	PlayerID  string      `json:"playerId"`
	Balance   money.Money `json:"balance"`
	Version   int64       `json:"version"`
	CreatedAt string      `json:"createdAt,omitempty"`
	UpdatedAt string      `json:"updatedAt,omitempty"`
}

type ledgerItemResponse struct {
	ID            string      `json:"id"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
	CreatedAt     string      `json:"createdAt"`
}

type ledgerResponse struct {
	Items      []ledgerItemResponse `json:"items"`
	NextCursor *string              `json:"nextCursor"`
}

func (h walletHandlers) post(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		h.writeError(w, r, err)
		return
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeProblem(w, errInvalidRequest.with("unexpected data after the JSON body"))
		return
	}

	created, err := h.open.Execute(r.Context(), openwallet.Input{
		PlayerID:       req.PlayerID,
		InitialBalance: req.InitialBalance,
		CorrelationID:  correlationIDFrom(r.Context()),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/wallets/"+string(created.ID()))
	writeJSON(w, http.StatusCreated, walletResponse{
		ID: string(created.ID()), PlayerID: string(created.PlayerID()), Balance: created.Balance(), Version: created.Version(),
	})
}

func (h walletHandlers) get(w http.ResponseWriter, r *http.Request) {
	got, err := h.queries.Wallet(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, walletResponse{
		ID: string(got.ID()), PlayerID: string(got.PlayerID()), Balance: got.Balance(), Version: got.Version(),
		CreatedAt: got.CreatedAt().UTC().Format(timeLayout), UpdatedAt: got.UpdatedAt().UTC().Format(timeLayout),
	})
}

func (h walletHandlers) ledger(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeProblem(w, errInvalidLimit)
			return
		}
		limit = n
	}
	page, err := h.queries.Ledger(r.Context(), r.PathValue("id"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		h.writeError(w, r, err)
		return
	}

	resp := ledgerResponse{Items: make([]ledgerItemResponse, len(page.Items))}
	for i, it := range page.Items {
		resp.Items[i] = ledgerItemResponse{
			ID: it.ID, TransactionID: it.TransactionID, Direction: string(it.Direction), Money: it.Money,
			BalanceBefore: it.BalanceBefore, BalanceAfter: it.BalanceAfter, WalletVersion: it.WalletVersion,
			CreatedAt: it.CreatedAt.UTC().Format(timeLayout),
		}
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

type reconciliationResponse struct {
	WalletID          string      `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

// reconciliation answers 200 whether or not the wallet is consistent: the check itself succeeded, and the verdict is in the body.
func (h walletHandlers) reconciliation(w http.ResponseWriter, r *http.Request) {
	report, err := h.reconcile.Execute(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: string(report.WalletID), StoredBalance: report.Stored, CalculatedBalance: report.Calculated,
		Difference: report.Difference, Consistent: report.Consistent, CheckedEntries: report.CheckedEntries,
	})
}

func (h walletHandlers) writeError(w http.ResponseWriter, r *http.Request, err error) {
	writeError(h.log, w, r, err)
}

// writeError maps domain and use case errors to the HTTP contract; routes with their own mapping handle those errors first.
func writeError(log *slog.Logger, w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	switch {
	case errors.Is(err, wallet.ErrAlreadyExists):
		writeProblem(w, errWalletExists)
	case errors.Is(err, wallet.ErrNotFound):
		writeProblem(w, errWalletNotFound)
	case errors.Is(err, wallet.ErrInvalidID):
		writeProblem(w, errInvalidID)
	case errors.Is(err, queries.ErrInvalidCursor):
		writeProblem(w, errInvalidCursor)
	case errors.Is(err, queries.ErrInvalidLimit):
		writeProblem(w, errInvalidLimit)
	case isMoneyError(err):
		writeProblem(w, errInvalidMoney)
	case errors.As(err, &tooLarge):
		writeProblem(w, errInvalidRequest.with("request body too large"))
	case isDecodeError(err):
		writeProblem(w, errInvalidRequest)
	case errors.Is(err, usecase.ErrTransient):
		log.Warn("transient failure", "correlationId", correlationIDFrom(r.Context()), "method", r.Method, "path", r.URL.Path, "error", err)
		writeProblem(w, errUnavailable)
	default:
		log.Error("request failed", "correlationId", correlationIDFrom(r.Context()), "method", r.Method, "path", r.URL.Path, "error", err)
		writeProblem(w, errInternal)
	}
}

var moneyErrors = []error{
	money.ErrUninitialized, money.ErrInvalidAmount, money.ErrInvalidCurrency,
	money.ErrOverflow, money.ErrNegative, money.ErrNotPositive, wager.ErrLossMustBeZero,
}

func isMoneyError(err error) bool {
	for _, target := range moneyErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// encoding/json reports unknown fields only as a plain string error.
func isDecodeError(err error) bool {
	var syntax *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	return errors.As(err, &syntax) || errors.As(err, &typeErr) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		strings.HasPrefix(err.Error(), "json: unknown field ")
}
