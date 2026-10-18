// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"fmt"
)

// ended returns err marked with the error of ctx once ctx has ended, and err as it is before.
func ended(ctx context.Context, err error) error {
	if end := ctx.Err(); end != nil {
		return fmt.Errorf("%w: %w", err, end)
	}
	return err
}
