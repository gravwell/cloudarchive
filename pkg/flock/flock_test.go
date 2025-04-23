//go:build !windows && !plan9 && !solaris
// +build !windows,!plan9,!solaris

/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package flock

import (
	"os"
	"testing"
)

const (
	prefix = `gravflock`
)

var (
	testFile *os.File
	testPath string
)

func TestStart(t *testing.T) {
	fout, err := os.CreateTemp(t.TempDir(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	testFile = fout
	testPath = fout.Name()
}

func TestLockUnlock(t *testing.T) {
	if testFile == nil {
		t.Fatal("Test file is nil")
	}
	if err := Flock(testFile, true); err != nil {
		t.Fatal(err)
	}
	if err := Funlock(testFile); err != nil {
		t.Fatal(err)
	}
	if err := Flock(testFile, true); err != nil {
		t.Fatal(err)
	}
	if err := Funlock(testFile); err != nil {
		t.Fatal(err)
	}
}

func TestLockExclusive(t *testing.T) {
	if testFile == nil {
		t.Fatal("Test file is nil")
	}
	if err := Flock(testFile, false); err != nil {
		t.Fatal(err)
	}
	if err := Flock(testFile, true); err != nil {
		t.Fatal(err)
	}
	if err := Funlock(testFile); err != nil {
		t.Fatal(err)
	}
}

func TestCleanup(t *testing.T) {
	if testFile == nil {
		return
	}
	if err := testFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(testPath); err != nil {
		t.Fatal(err)
	}
}
