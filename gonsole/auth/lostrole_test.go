// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"slices"
	"testing"

	"github.com/gopherium/framework/gonsole"
	"github.com/gopherium/framework/gonsole/testkit"
)

// unchecked returns a program over the world at address whose account writes name no capability.
func unchecked(address string) gonsole.Program {
	var reads rolesRead
	return program(address, standingCommands(config(&reads, ""))...)
}

// asManager is the acting account the overseen runs name.
var asManager = []string{"-as", "manager@example.com"}

func TestEnableRefusesAnAccountWhoseRoleNobodyDeclares(t *testing.T) {
	t.Parallel()

	const (
		undeclared = "the account author@example.com holds the undeclared role guest, " +
			"give it a declared role before enabling it"
		roleless = "the account author@example.com holds no role, give it a declared role before enabling it"
	)
	tests := []struct {
		name  string
		setup []change
		over  func(address string) gonsole.Program
		actor []string
		want  string
	}{
		{"a disabled account under a role the roles leave out",
			[]change{holding("author@example.com", "guest"), disabling("author@example.com")}, overseen, asManager,
			undeclared},
		{"a disabled account with no role",
			[]change{holding("author@example.com", ""), disabling("author@example.com")}, overseen, asManager, roleless},
		{"an enabled account under a role the roles leave out", []change{holding("author@example.com", "guest")},
			overseen, asManager, undeclared},
		{"a disabled account under a role the roles leave out where no capability is named",
			[]change{holding("author@example.com", "guest"), disabling("author@example.com")}, unchecked, nil,
			undeclared},
		{"a disabled account with no role where no capability is named",
			[]change{holding("author@example.com", ""), disabling("author@example.com")}, unchecked, nil, roleless},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			address := managed(t)
			run(t, address, tt.setup...)
			before := standingOf(t, address, "author@example.com")
			line := slices.Concat([]string{"account:enable", "author@example.com"}, tt.actor)

			refuses(t, tt.over(address), "myapp: "+tt.want+"\n", line...)
			refuses(t, tt.over(address), "myapp: "+tt.want+"\n", append(line, "-yes")...)

			if held := records(t, address); len(held) != 0 {
				t.Errorf("records = %v, want none", held)
			}
			if after := standingOf(t, address, "author@example.com"); after != before {
				t.Errorf("author@example.com holds %s, want %s kept", after, before)
			}
		})
	}
}

func TestEnableLetsAnAccountBackOnceItHoldsADeclaredRole(t *testing.T) {
	t.Parallel()

	address := managed(t)
	run(t, address, holding("author@example.com", "guest"), disabling("author@example.com"))
	p := overseen(address)

	moved := testkit.Run(t, p, "", slices.Concat([]string{"account:role", "author@example.com", "author", "-yes"},
		asManager)...)
	got := testkit.Run(t, p, "", slices.Concat([]string{"account:enable", "author@example.com", "-yes"}, asManager)...)

	if moved.Code != gonsole.ExitDone || got != (testkit.Result{Code: gonsole.ExitDone,
		Stdout: "enabled author@example.com\n"}) {
		t.Errorf("Run() = %+v, then %+v, want the role set, then the account enabled", moved, got)
	}
	if after := standingOf(t, address, "author@example.com"); after != `role "author", disabled false` {
		t.Errorf("author@example.com holds %s, want an enabled author", after)
	}
}
