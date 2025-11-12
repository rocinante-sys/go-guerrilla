package frontends

import (
	"errors"
)

type EmailError error

var (
	NoSuchUser          = EmailError(errors.New("no such user"))
	StorageNotAvailable = EmailError(errors.New("storage not available"))
	StorageTooBusy      = EmailError(errors.New("storage too busy"))
	StorageTimeout      = EmailError(errors.New("storage timeout"))
	QuotaExceeded       = EmailError(errors.New("quota exceeded"))
	UserSuspended       = EmailError(errors.New("user suspended"))
	StorageError        = EmailError(errors.New("storage error"))
)
