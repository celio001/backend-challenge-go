package processwager

import "errors"

// Correctable errors: nothing is stored and the provider may resend a fixed request.
var (
	ErrValidation            = errors.New("processwager: invalid request")
	ErrMissingIdempotencyKey = errors.New("processwager: idempotency key is required")
	ErrKindNotAllowed        = errors.New("processwager: kind not allowed")
	ErrPlayerWalletMismatch  = errors.New("processwager: wallet belongs to another player")
	ErrIdempotencyKeyReused  = errors.New("processwager: idempotency key reused with a different payload")
	ErrExternalIDConflict    = errors.New("processwager: external transaction id already used with another idempotency key")
)
