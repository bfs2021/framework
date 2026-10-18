// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"fmt"

	"github.com/gopherium/framework/gonsole"
)

// DeclaredRoles returns the Roles hook answering registry after the plugins, the roles carrying capability privileged.
func DeclaredRoles(registry interface {
	Roles() []string
	CapabilitiesOf(role string) []string
	HoldersOf(capability string) []string
}, capability string) func(ctx context.Context, call gonsole.Call) (Roles, error) {
	return func(ctx context.Context, call gonsole.Call) (Roles, error) {
		loaded, err := call.Plugins(ctx)
		if err != nil {
			return Roles{}, err
		}
		if loaded.Failed != nil {
			return Roles{}, fmt.Errorf("register plugins: %w", loaded.Failed)
		}
		holders := registry.HoldersOf(capability)
		if len(holders) == 0 {
			return Roles{}, fmt.Errorf("no role carries the capability %q", capability)
		}
		return RolesFrom(registry, holders), nil
	}
}
