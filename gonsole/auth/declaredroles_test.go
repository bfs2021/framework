// SPDX-License-Identifier: Apache-2.0

package auth_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"

	"github.com/gopherium/framework/gonsole"
	"github.com/gopherium/framework/gonsole/auth"
	"github.com/gopherium/framework/gonsole/testkit"
)

// pluginsHook is what a program runs to register its plugins.
type pluginsHook = func(ctx context.Context, call gonsole.Call) (gonsole.Loaded, error)

// reporting returns roles:report, which prints what hook answers for the call.
func reporting(hook func(context.Context, gonsole.Call) (auth.Roles, error)) gonsole.Command {
	return gonsole.Command{
		Name:    "roles:report",
		Summary: "print the roles",
		Run: func(ctx context.Context, call gonsole.Call) error {
			roles, err := hook(ctx, call)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(call.Stdout, "known %v, privileged %v, capabilities %v\n",
				roles.Known, roles.Privileged, roles.Capabilities)
			return err
		},
	}
}

// declaring returns the plugins hook that declares archivist carrying manage_users into table.
func declaring(table registry) pluginsHook {
	return func(context.Context, gonsole.Call) (gonsole.Loaded, error) {
		table["archivist"] = []string{"manage_users"}
		return gonsole.Loaded{}, nil
	}
}

func TestDeclaredRolesAnswersTheRegistryOnceThePluginsRegistered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		plugins    func(table registry) pluginsHook
		capability string
		want       string
	}{
		{"a program without plugins", nil, "manage_users", "known [admin author editor], privileged [admin], " +
			"capabilities map[admin:[manage_users change_others_work] author:[] editor:[change_others_work]]\n"},
		{"plugins declaring a role", declaring, "manage_users", "known [admin archivist author editor], " +
			"privileged [admin archivist], capabilities map[admin:[manage_users change_others_work] " +
			"archivist:[manage_users] author:[] editor:[change_others_work]]\n"},
		{"a capability two roles carry", nil, "change_others_work", "known [admin author editor], " +
			"privileged [admin editor], " +
			"capabilities map[admin:[manage_users change_others_work] author:[] editor:[change_others_work]]\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			table := maps.Clone(capableRegistry)
			p := program(unreachable, reporting(auth.DeclaredRoles(table, tt.capability)))
			if tt.plugins != nil {
				p.Plugins = tt.plugins(table)
			}

			got := testkit.Run(t, p, "", "roles:report")

			if want := (testkit.Result{Code: gonsole.ExitDone, Stdout: tt.want}); got != want {
				t.Errorf("Run() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestDeclaredRolesRefusesPluginsThatFailedToLoad(t *testing.T) {
	t.Parallel()

	locked := errors.New("the plugin table is locked")
	tests := []struct {
		name   string
		loaded gonsole.Loaded
		err    error
		want   string
	}{
		{"a plugin that failed to register", gonsole.Loaded{Failed: locked}, nil,
			"myapp: register plugins: the plugin table is locked\n"},
		{"a registration that failed", gonsole.Loaded{}, locked, "myapp: the plugin table is locked\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := program(unreachable, reporting(auth.DeclaredRoles(maps.Clone(capableRegistry), "manage_users")))
			p.Plugins = func(context.Context, gonsole.Call) (gonsole.Loaded, error) { return tt.loaded, tt.err }

			got := testkit.Run(t, p, "", "roles:report")

			if want := (testkit.Result{Code: gonsole.ExitFailed, Stderr: tt.want}); got != want {
				t.Errorf("Run() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestDeclaredRolesRefusesACapabilityNoRoleCarries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		plugins    func(table registry) pluginsHook
		capability string
		want       string
	}{
		{"a capability no role carries", nil, "publish_reports",
			"myapp: no role carries the capability \"publish_reports\"\n"},
		{"a capability no plugin role carries either", declaring, "publish_reports",
			"myapp: no role carries the capability \"publish_reports\"\n"},
		{"no capability named", nil, "", "myapp: no role carries the capability \"\"\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			table := maps.Clone(capableRegistry)
			p := program(unreachable, reporting(auth.DeclaredRoles(table, tt.capability)))
			if tt.plugins != nil {
				p.Plugins = tt.plugins(table)
			}

			got := testkit.Run(t, p, "", "roles:report")

			if want := (testkit.Result{Code: gonsole.ExitFailed, Stderr: tt.want}); got != want {
				t.Errorf("Run() = %+v, want %+v", got, want)
			}
		})
	}
}

func TestDeclaredRolesKeepsTheLastPrivilegedAccountOfACapabilityNoRoleCarries(t *testing.T) {
	t.Parallel()

	address := seeded(t)
	p := program(address, auth.SetRole(auth.Config{Roles: auth.DeclaredRoles(maps.Clone(capableRegistry),
		"publish_reports"), Stores: memoryStores}))

	got := testkit.Run(t, p, "", "account:role", "admin@example.com", "author", "-yes")

	want := testkit.Result{Code: gonsole.ExitFailed, Stderr: "myapp: no role carries the capability \"publish_reports\"\n"}
	if got != want {
		t.Errorf("Run() = %+v, want %+v", got, want)
	}
	if held := account(t, storeAt(t, address), "admin@example.com"); held.Role != "admin" {
		t.Errorf("role = %q, want admin", held.Role)
	}
}

func TestDeclaredRolesServesAsTheRolesOfTheAccountCommands(t *testing.T) {
	t.Parallel()

	address := seeded(t)
	table := maps.Clone(capableRegistry)
	p := program(address, auth.SetRole(auth.Config{Roles: auth.DeclaredRoles(table, "manage_users"),
		Stores: memoryStores}))
	p.Plugins = declaring(table)

	got := testkit.Run(t, p, "", "account:role", "editor@example.com", "archivist", "-yes")

	if want := (testkit.Result{Code: gonsole.ExitDone, Stdout: "set editor@example.com to archivist\n"}); got != want {
		t.Errorf("Run() = %+v, want %+v", got, want)
	}
	if held := account(t, storeAt(t, address), "editor@example.com"); held.Role != "archivist" {
		t.Errorf("role = %q, want archivist", held.Role)
	}
}
