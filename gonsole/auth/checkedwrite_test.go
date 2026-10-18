// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/gopherium/gouncer"

	"github.com/gopherium/framework/gonsole"
	"github.com/gopherium/framework/gonsole/auth"
)

// roleChangingStore is an account store that gives the account at email the role right after it reads that account.
type roleChangingStore struct {
	auth.Accounts
	email, role string
}

// UserByEmail returns the account at email as read, then gives it the store's role when it is the store's account.
func (s roleChangingStore) UserByEmail(ctx context.Context, email string) (gouncer.User, error) {
	held, err := s.Accounts.UserByEmail(ctx, email)
	if err != nil || email != s.email {
		return held, err
	}
	return held, s.SetUserRole(ctx, held.ID, s.role, nil)
}

// withRoleChangedAfterRead returns a program like overseen whose stores give the account at email the role once read.
func withRoleChangedAfterRead(address, email, role string) gonsole.Program {
	cfg := reaching()
	cfg.Stores = func(ctx context.Context, call gonsole.Call) (auth.Stores, func(context.Context) error, error) {
		stores, release, err := memoryStores(ctx, call)
		stores.Accounts = roleChangingStore{Accounts: stores.Accounts, email: email, role: role}
		return stores, release, err
	}
	return authorizing(address, cfg, nil, auth.Commands(cfg)...)
}

func TestAccountWritesRefuseAnAccountWhoseRoleChangedAfterTheCheck(t *testing.T) {
	t.Parallel()

	const changed = "myapp: the role of author@example.com changed after the check, run the command again\n"
	tests := []struct {
		name  string
		setup []change
		args  []string
	}{
		{"a role change", nil, []string{"account:role", "author@example.com", "manager"}},
		{"a disable", nil, []string{"account:disable", "author@example.com"}},
		{"an enable", []change{disabling("author@example.com")}, []string{"account:enable", "author@example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			address := managed(t)
			run(t, address, tt.setup...)
			disabled := account(t, storeAt(t, address), "author@example.com").Disabled

			refuses(t, withRoleChangedAfterRead(address, "author@example.com", "admin"), changed,
				slices.Concat(tt.args, []string{"-yes"}, asManager)...)

			if held := records(t, address); len(held) != 0 {
				t.Errorf("records = %v, want none", held)
			}
			want := fmt.Sprintf("role %q, disabled %t", "admin", disabled)
			if after := standingOf(t, address, "author@example.com"); after != want {
				t.Errorf("author@example.com holds %s, want %s, the changed role and the standing kept", after, want)
			}
		})
	}
}
