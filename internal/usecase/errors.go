package usecase

import "errors"

var (
	// ErrInboxHashMismatch means a message id was redelivered with different content.
	ErrInboxHashMismatch = errors.New("usecase: inbox message id reused with a different payload")
	// ErrTransient marks infrastructure failures that a retry of the whole unit of work can resolve.
	ErrTransient = errors.New("usecase: transient failure")
	// ErrPermanent marks infrastructure failures that retrying cannot fix; the operation is recorded as FAILED.
	ErrPermanent = errors.New("usecase: permanent infrastructure failure")
	// ErrStaleWallet means the wallet version moved between read and write; it is always transient.
	ErrStaleWallet = errors.New("usecase: wallet changed concurrently")
)
