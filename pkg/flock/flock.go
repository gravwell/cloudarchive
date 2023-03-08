//go:build !windows && !plan9 && !solaris
// +build !windows,!plan9,!solaris

//this package is based on the flock implementation used in boltdb
//which is MIT licensed and available at:
//	https://github.com/boltdb/bolt/blob/master/bolt_unix.go
/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package flock

import (
	"errors"
	"os"
	"syscall"
)

var (
	ErrTimeout = errors.New("Timeout")
	ErrLocked  = errors.New("File is already locked")
)

// Flock locks a file for this process, this DOES NOT prevent the same process
// from opening the
func Flock(f *os.File, exclusive bool) error {
	var lock syscall.Flock_t
	lock.Start = 0
	lock.Len = 0
	lock.Pid = 0
	lock.Whence = 0
	lock.Pid = 0
	if exclusive {
		lock.Type = syscall.F_WRLCK
	} else {
		lock.Type = syscall.F_RDLCK
	}
	err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lock)
	if err == nil {
		return nil
	} else if err == syscall.EAGAIN {
		return ErrLocked
	}
	return err
}

// Funlock releases a lock held on a file descriptor
func Funlock(f *os.File) error {
	var lock syscall.Flock_t
	lock.Start = 0
	lock.Len = 0
	lock.Type = syscall.F_UNLCK
	lock.Whence = 0
	return syscall.FcntlFlock(uintptr(f.Fd()), syscall.F_SETLK, &lock)

}
