package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	lua "github.com/yuin/gopher-lua"
)

// sameLua compares two values the way the uploader reads them: tables key by key.
func sameLua(a, b lua.LValue) error {
	ta, okA := a.(*lua.LTable)
	tb, okB := b.(*lua.LTable)
	if okA != okB {
		return fmt.Errorf("%s vs %s", a.Type(), b.Type())
	}
	if !okA {
		if a != b {
			return fmt.Errorf("%q vs %q", a.String(), b.String())
		}
		return nil
	}
	var err error
	n := 0
	ta.ForEach(func(k, v lua.LValue) {
		n++
		if err == nil {
			if e := sameLua(v, tb.RawGet(k)); e != nil {
				err = fmt.Errorf("[%s] %v", k.String(), e)
			}
		}
	})
	m := 0
	tb.ForEach(func(_, _ lua.LValue) { m++ })
	if err == nil && n != m {
		err = fmt.Errorf("%d keys vs %d", n, m)
	}
	return err
}

func compareWithLua(t *testing.T, data []byte, globals ...string) {
	t.Helper()
	fast := lua.NewState()
	defer fast.Close()
	t0 := time.Now()
	if err := loadSavedVarsData(fast, data); err != nil {
		t.Fatal(err)
	}
	t.Logf("data reader: %v", time.Since(t0))
	slow := lua.NewState()
	defer slow.Close()
	t0 = time.Now()
	if err := slow.DoString(string(data)); err != nil {
		t.Fatal(err)
	}
	t.Logf("lua compiler: %v", time.Since(t0))
	for _, g := range globals {
		if err := sameLua(slow.GetGlobal(g), fast.GetGlobal(g)); err != nil {
			t.Fatalf("%s: %v", g, err)
		}
	}
}

func TestSavedVarsData(t *testing.T) {
	data := []byte(`
VoidScoutDB = {
	["sim_opt_in"] = true,
	["off"] = false,
	["gone"] = nil,
	["n"] = -12.5e-3,
	["big"] = 1793304458,
	["hex"] = 0x1F,
	["s"] = "a \"q\" \\ tab\tnl\nend \226\128\148",
	[7] = "seven",
	["list"] = {
		{
			["id"] = 1,
		}, -- [1]
		"two", -- [2]
		true, -- [3]
	},
	plain = 'single',
}
Other = nil
Empty = {
}
`)
	compareWithLua(t, data, "VoidScoutDB", "Other", "Empty")
	L := lua.NewState()
	defer L.Close()
	// Input that Lua reads differently or rejects must fail here, so the caller falls back.
	for _, bad := range []string{
		"X = { [1] = function() end }",
		`X = { ["a"] = 1 }  Y = { --[[ block ]] }`,
		"X=1\n--[=[\nX=2\n--]=]",
		"X = {1, [1] = 2}",
		"X = {[1] = 2, 1}",
		"X = {[nil] = 1}",
		"X = 0010",
		"X = \"a\rb\"",
		"X = \"a\\\n\rb\"",
		"end = 1",
		"X = {true = 1}",
		"X = {1 2}",
		"X = {",
	} {
		if loadSavedVarsData(L, []byte(bad)) == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	if L.GetGlobal("X") != lua.LNil {
		t.Fatal("a failed file left globals set")
	}
	// A comment ends at CR as well as LF.
	compareWithLua(t, []byte("X = 1 --c\rX = 2"), "X")
	compareWithLua(t, []byte("X = {1, 2, [4] = 4, [2.5] = 1}"), "X")
}

// SV=<path to VoidScout.lua> go test -run TestSavedVarsReal -v  (slow: runs the Lua compiler too)
func TestSavedVarsReal(t *testing.T) {
	p := os.Getenv("SV")
	if p == "" {
		t.Skip("SV not set")
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	compareWithLua(t, bytes.Clone(data), "VoidScoutDB")
}
