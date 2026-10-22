//go:build windows

package main

import (
	"io"
	"os"
	"syscall"
)

// readShared reads a whole file while letting other programs delete or replace
// it mid-read. WoW saves an addon by writing <name>.lua.tmp and swapping it over
// <name>.lua; os.Open omits FILE_SHARE_DELETE, so if we have the file open at
// that moment the swap fails and WoW silently keeps the OLD save -- that's how a
// timed +2 was nearly lost on 2026-10-22 (VoidScout.lua.tmp left behind).
func readShared(path string) ([]byte, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	return io.ReadAll(f)
}
