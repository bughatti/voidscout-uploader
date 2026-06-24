//go:build !windows

package main

import "syscall"

// relaunchSysProcAttr detaches the successor process into its own session during
// an auto-update so it survives the parent exiting.
func relaunchSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
