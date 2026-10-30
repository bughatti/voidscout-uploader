package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Uploads follow the addon's current consent; sim sharing needs uploads allowed too.
func TestReadConsent(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name, sv     string
		allowed, sim bool
	}{
		{"allowed with sim", `VoidScoutDB = { ["consent"] = { ["version"] = 3, ["choice"] = "allowed" }, ["sim_opt_in"] = true }`, true, true},
		{"allowed, sim off", `VoidScoutDB = { ["consent"] = { ["version"] = 3, ["choice"] = "allowed" }, ["sim_opt_in"] = false }`, true, false},
		{"local-only", `VoidScoutDB = { ["consent"] = { ["version"] = 3, ["choice"] = "local" }, ["sim_opt_in"] = true }`, false, false},
		{"outdated consent", `VoidScoutDB = { ["consent"] = { ["version"] = 1, ["choice"] = "allowed" } }`, false, false},
		{"no choice yet", `VoidScoutDB = { ["sim_opt_in"] = true }`, false, false},
	}
	for _, c := range cases {
		p := filepath.Join(dir, "VoidScout.lua")
		if err := os.WriteFile(p, []byte(c.sv+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if a, s := readConsent(p); a != c.allowed || s != c.sim {
			t.Errorf("%s: got allowed=%v sim=%v, want %v %v", c.name, a, s, c.allowed, c.sim)
		}
	}
	if a, s := readConsent(filepath.Join(dir, "missing.lua")); a || s {
		t.Error("a missing file allowed uploads")
	}
}
