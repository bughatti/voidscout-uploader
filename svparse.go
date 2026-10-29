package main

import (
	"fmt"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
)

// loadSavedVarsData sets the globals of a SavedVariables file on L without running it as
// Lua. gopher-lua's compiler looks every constant up with a linear scan, so compiling a
// 32 MB VoidScout.lua takes minutes (O(n^2) in its constants). WoW writes SavedVariables
// as plain data (Name = value, with tables, strings, numbers, booleans and nil), and this
// reads that in one pass. Anything else is an error, and the caller falls back to Lua.
func loadSavedVarsData(L *lua.LState, data []byte) error {
	p := &svParser{L: L, b: data}
	globals := map[string]lua.LValue{}
	for {
		p.space()
		if p.i >= len(p.b) {
			break
		}
		name := p.name()
		if name == "" {
			return p.fail("expected a global name")
		}
		p.space()
		if !p.eat('=') {
			return p.fail("expected =")
		}
		v, err := p.value()
		if err != nil {
			return err
		}
		globals[name] = v
		p.space()
		p.eat(';')
	}
	for k, v := range globals { // set only once the whole file has parsed
		L.SetGlobal(k, v)
	}
	return nil
}

type svParser struct {
	L *lua.LState
	b []byte
	i int
}

func (p *svParser) fail(msg string) error {
	return fmt.Errorf("savedvars data: %s at byte %d", msg, p.i)
}

// space skips whitespace and -- line comments (WoW writes "-- [N]" after array items).
func (p *svParser) space() {
	for p.i < len(p.b) {
		switch c := p.b[p.i]; {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			p.i++
		case c == '-' && p.i+1 < len(p.b) && p.b[p.i+1] == '-':
			if p.i+3 < len(p.b) && p.b[p.i+2] == '[' && (p.b[p.i+3] == '[' || p.b[p.i+3] == '=') {
				return // a block comment: leave it so the caller fails and falls back
			}
			for p.i < len(p.b) && p.b[p.i] != '\n' && p.b[p.i] != '\r' {
				p.i++
			}
		default:
			return
		}
	}
}

func (p *svParser) eat(c byte) bool {
	if p.i < len(p.b) && p.b[p.i] == c {
		p.i++
		return true
	}
	return false
}

func (p *svParser) ident() string {
	start := p.i
	for p.i < len(p.b) {
		c := p.b[p.i]
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || p.i > start && c >= '0' && c <= '9' {
			p.i++
			continue
		}
		break
	}
	return string(p.b[start:p.i])
}

var luaReserved = map[string]bool{"and": true, "break": true, "do": true, "else": true, "elseif": true,
	"end": true, "false": true, "for": true, "function": true, "if": true, "in": true, "local": true,
	"nil": true, "not": true, "or": true, "repeat": true, "return": true, "then": true, "true": true,
	"until": true, "while": true}

// name reads an identifier that may be assigned to (not a reserved word).
func (p *svParser) name() string {
	start := p.i
	s := p.ident()
	if luaReserved[s] {
		p.i = start
		return ""
	}
	return s
}

func (p *svParser) value() (lua.LValue, error) {
	p.space()
	if p.i >= len(p.b) {
		return nil, p.fail("unexpected end")
	}
	switch c := p.b[p.i]; {
	case c == '{':
		return p.table()
	case c == '"' || c == '\'':
		s, err := p.str()
		return lua.LString(s), err
	case c == '-' || c == '.' || c >= '0' && c <= '9':
		return p.number()
	}
	start := p.i
	switch p.ident() {
	case "true":
		return lua.LTrue, nil
	case "false":
		return lua.LFalse, nil
	case "nil":
		return lua.LNil, nil
	}
	p.i = start
	return nil, p.fail("unexpected value")
}

func (p *svParser) table() (lua.LValue, error) {
	p.i++ // {
	t := p.L.NewTable()
	n := 0 // positional items count from 1, as in a Lua table constructor
	// Lua stores positional items after the keyed ones, so {1, [1] = 2} keeps 1. Rather than
	// copy that order, a keyed field on a positional index is an error (falls back to Lua).
	var numKeys []float64
	for {
		p.space()
		if p.eat('}') {
			for _, k := range numKeys {
				if k >= 1 && k <= float64(n) && k == float64(int(k)) {
					return nil, p.fail("keyed field on a positional index")
				}
			}
			return t, nil
		}
		var key lua.LValue
		if p.eat('[') {
			k, err := p.value()
			if err != nil {
				return nil, err
			}
			p.space()
			if !p.eat(']') {
				return nil, p.fail("expected ]")
			}
			p.space()
			if !p.eat('=') {
				return nil, p.fail("expected =")
			}
			key = k
		} else if start := p.i; p.name() != "" {
			name := string(p.b[start:p.i])
			p.space()
			if p.eat('=') {
				key = lua.LString(name)
			} else {
				p.i = start // a bare true/false/nil item
			}
		}
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		if key == nil {
			n++
			t.RawSetInt(n, v)
		} else {
			if key == lua.LNil {
				return nil, p.fail("nil table key")
			}
			if f, ok := key.(lua.LNumber); ok {
				if f != f {
					return nil, p.fail("NaN table key")
				}
				numKeys = append(numKeys, float64(f))
			}
			t.RawSet(key, v)
		}
		p.space()
		if !p.eat(',') && !p.eat(';') {
			p.space()
			if p.i >= len(p.b) || p.b[p.i] != '}' { // the loop's top consumes it
				return nil, p.fail("expected , or }")
			}
		}
	}
}

func (p *svParser) number() (lua.LValue, error) {
	start := p.i
	if p.b[p.i] == '-' {
		p.i++
	}
	for p.i < len(p.b) {
		c := p.b[p.i]
		if c >= '0' && c <= '9' || c == '.' || c == 'e' || c == 'E' || c == 'x' || c == 'X' ||
			c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' ||
			(c == '+' || c == '-') && (p.b[p.i-1] == 'e' || p.b[p.i-1] == 'E') {
			p.i++
			continue
		}
		break
	}
	s := string(p.b[start:p.i])
	if d := strings.TrimPrefix(s, "-"); len(d) > 1 && d[0] == '0' && d[1] >= '0' && d[1] <= '9' {
		return nil, p.fail("leading zero") // gopher-lua reads 010 as octal
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return lua.LNumber(f), nil
	}
	if n, err := strconv.ParseInt(s, 0, 64); err == nil { // 0x hex integers
		return lua.LNumber(n), nil
	}
	return nil, p.fail("bad number " + strconv.Quote(s))
}

// str reads a quoted Lua 5.1 string with its escapes.
func (p *svParser) str() (string, error) {
	q := p.b[p.i]
	p.i++
	start := p.i
	for p.i < len(p.b) && p.b[p.i] != q && p.b[p.i] != '\\' && p.b[p.i] != '\n' && p.b[p.i] != '\r' {
		p.i++
	}
	if p.i < len(p.b) && p.b[p.i] == q { // the common case: no escapes
		s := string(p.b[start:p.i])
		p.i++
		return s, nil
	}
	out := append([]byte(nil), p.b[start:p.i]...)
	for p.i < len(p.b) {
		c := p.b[p.i]
		p.i++
		switch {
		case c == q:
			return string(out), nil
		case c == '\n' || c == '\r':
			return "", p.fail("newline in string")
		case c != '\\':
			out = append(out, c)
			continue
		}
		if p.i >= len(p.b) {
			break
		}
		e := p.b[p.i]
		p.i++
		switch e {
		case '\n', '\r': // an escaped line break: Lua folds CR/LF pairs; WoW never writes it
			return "", p.fail("escaped line break")
		case 'n':
			out = append(out, '\n')
		case 't':
			out = append(out, '\t')
		case 'r':
			out = append(out, '\r')
		case 'a':
			out = append(out, '\a')
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'v':
			out = append(out, '\v')
		case '\\', '"', '\'':
			out = append(out, e)
		default:
			if e < '0' || e > '9' {
				return "", p.fail("bad escape")
			}
			n := int(e - '0')
			for k := 0; k < 2 && p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9'; k++ {
				n = n*10 + int(p.b[p.i]-'0')
				p.i++
			}
			if n > 255 {
				return "", p.fail("escape too large")
			}
			out = append(out, byte(n))
		}
	}
	return "", p.fail("unterminated string")
}
