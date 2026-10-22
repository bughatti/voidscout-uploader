//go:build !windows

package main

import "os"

// readShared: on macOS/Linux an open file never blocks another program from
// replacing it, so a plain read is enough. (See readshared_windows.go.)
func readShared(path string) ([]byte, error) {
	return os.ReadFile(path)
}
