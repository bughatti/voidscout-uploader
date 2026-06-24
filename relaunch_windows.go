//go:build windows

package main

import "syscall"

// relaunchSysProcAttr detaches the successor process during an auto-update so no
// console window flashes (the daemon runs hidden) and it survives the parent
// exiting. CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP.
func relaunchSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x08000000 | 0x00000200,
	}
}
