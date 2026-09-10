package core

import (
	"errors"
	"fmt"
)

// MutationErrorKind is the stable caller-facing class of a config mutation
// failure. HTTP adapters use it without depending on human-readable wording.
type MutationErrorKind uint8

const (
	MutationInvalid MutationErrorKind = iota + 1
	MutationNotFound
	MutationConflict
)

// MutationError preserves the detailed error while attaching a stable class.
type MutationError struct {
	Kind MutationErrorKind
	Err  error
}

func (e *MutationError) Error() string { return e.Err.Error() }
func (e *MutationError) Unwrap() error { return e.Err }

func mutationError(kind MutationErrorKind, err error) error {
	if err == nil {
		return nil
	}
	return &MutationError{Kind: kind, Err: err}
}

func mutationErrorf(kind MutationErrorKind, format string, args ...any) error {
	return &MutationError{Kind: kind, Err: fmt.Errorf(format, args...)}
}

// ClassifyMutationError returns the outermost stable mutation class.
func ClassifyMutationError(err error) (MutationErrorKind, bool) {
	var target *MutationError
	if !errors.As(err, &target) {
		return 0, false
	}
	return target.Kind, true
}
