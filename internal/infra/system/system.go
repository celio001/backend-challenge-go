package system

import (
	"time"

	"github.com/google/uuid"
)

type Clock struct{}

func (Clock) Now() time.Time { return time.Now().UTC() }

// IDs generates UUIDv7, which is time-ordered and keeps database indexes compact.
type IDs struct{}

// NewID panics if the system entropy source fails: there is no safe way to continue without unique identifiers.
func (IDs) NewID() string { return uuid.Must(uuid.NewV7()).String() }
