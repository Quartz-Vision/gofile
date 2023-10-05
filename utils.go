package gofile

import (
	"syscall"
)

// GetFilesLimit returns the soft limit for simultaneously opened files
func GetFilesLimit() (nLimit uint64, err error) {
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		return 0, err
	}
	return limit.Cur, nil
}
