// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"errors"
	"testing"
)

func TestEndedMarksAnErrorOnlyOnceItsContextEnded(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	if got := ended(t.Context(), errStepFailed); got != errStepFailed {
		t.Errorf("ended() under a live context = %v, want the error as it is", got)
	}
	got := ended(cancelled, errStepFailed)
	if !errors.Is(got, errStepFailed) || !errors.Is(got, context.Canceled) {
		t.Errorf("ended() under a cancelled context = %v, want an error matching both the error and context.Canceled",
			got)
	}
	if want := errStepFailed.Error() + ": " + context.Canceled.Error(); got == nil || got.Error() != want {
		t.Errorf("ended() under a cancelled context = %v, want %q", got, want)
	}
}
