package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The uploading character comes from VoidScoutDB.me when the addon recorded
// it, not from "whoever has the most fight rows".
func TestContributorPrefersMe(t *testing.T) {
	dir := t.TempDir()
	sv := filepath.Join(dir, "VoidScout.lua")
	src := `VoidScoutDB = {
  me = { key = "Vede-Elune" },
  scores = {
    ["Groupmate-Zuljin"] = { fights = {} },
  },
}`
	if err := os.WriteFile(sv, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	_, contributor, err := parseSavedVars(sv)
	if err != nil {
		t.Fatal(err)
	}
	if contributor != "Vede-Elune" {
		t.Fatalf("contributor = %q, want Vede-Elune", contributor)
	}
}

// Older addons (no VoidScoutDB.me) keep the old heuristic.
func TestContributorFallbackWithoutMe(t *testing.T) {
	dir := t.TempDir()
	sv := filepath.Join(dir, "VoidScout.lua")
	src := `VoidScoutDB = { scores = {} }`
	if err := os.WriteFile(sv, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	_, contributor, err := parseSavedVars(sv)
	if err != nil {
		t.Fatal(err)
	}
	if contributor != "" {
		t.Fatalf("contributor = %q, want empty with no fights", contributor)
	}
}
