/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package auth

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
)

const (
	testUser1Password        = `password`
	testUser1ID       uint64 = 123456789
	testUser1IDS      string = `123456789`
	testUser1                = `123456789:$2a$12$5fitJ4VGGqU9xQMZX..UxOVvvyFj/Fw3ZVMyRMFWLFnq4eOVSN1YG`
	testUser2Password        = `password12345`
	testUser2ID              = 13371337
	testUser2IDS      string = `13371337`
	testUser2                = `13371337:$2a$10$rMk0Usz6tkteuRsyvRk6mej7eEhV/EKmBklxDn9YCdV4r95ByGEae`
)

var (
	tdir string
)

func TestMain(m *testing.M) {
	var err error
	tdir, err = ioutil.TempDir(os.TempDir(), "gravauth")
	if err != nil {
		log.Fatal(err)
	}
	r := m.Run()
	if err := os.RemoveAll(tdir); err != nil {
		log.Fatal(err)
	}
	os.Exit(r)
}

func TestNew(t *testing.T) {
	//test with no file
	if _, err := NewAuthModule(filepath.Join(tdir, "empty")); err != nil {
		t.Fatal(err)
	}
	//test with an existing empty file
	if err := testFile(filepath.Join(tdir, "test")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAuthModule(filepath.Join(tdir, "test")); err != nil {
		t.Fatal(err)
	}

	//test with some existing data
	if err := dropTestFile(filepath.Join(tdir, "test2")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewAuthModule(filepath.Join(tdir, "test2")); err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	pth := filepath.Join(tdir, "test3")
	//test with some existing data
	if err := dropTestFile(pth); err != nil {
		t.Fatal(err)
	}
	a, err := NewAuthModule(pth)
	if err != nil {
		t.Fatal(err)
	}
	//test a manual load
	uhs, err := a.load()
	if err != nil {
		t.Fatal(err)
	} else if len(uhs) != 2 {
		t.Fatalf("Load count is invalid: %d != 2", len(uhs))
	}
	//test an authentication
	if cid, err := a.Authenticate(testUser1IDS, testUser1Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser1ID {
		t.Fatal("bad userid")
	}
	if cid, err := a.Authenticate(testUser2IDS, testUser2Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser2ID {
		t.Fatal("bad userid")
	}

	//test some bad authentications
	if _, err := a.Authenticate(testUser1IDS, ``); err == nil {
		t.Fatal("failed to catch bad params")
	}
	if _, err := a.Authenticate(``, testUser1Password); err == nil {
		t.Fatal("failed to catch bad params")
	}
	if _, err := a.Authenticate(``, ``); err == nil {
		t.Fatal("failed to catch bad params")
	}
	if _, err := a.Authenticate(testUser1Password, `foobar`); err == nil {
		t.Fatal("failed to catch bad params")
	}
}

func TestAdd(t *testing.T) {
	pth := filepath.Join(tdir, "test4")
	//test with some existing data
	if err := dropTestFile(pth); err != nil {
		t.Fatal(err)
	}
	a, err := NewAuthModule(pth)
	if err != nil {
		t.Fatal(err)
	}
	//check an auth real quick
	if cid, err := a.Authenticate(testUser1IDS, testUser1Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser1ID {
		t.Fatal("bad userid")
	}
	//add a new user
	if err = a.AddUser(10, `password`, 10); err != nil {
		t.Fatal(err)
	}
	//attempt to add with an existing userid
	if err = a.AddUser(testUser1ID, `password`, 10); err == nil {
		t.Fatal("failed to catch collision")
	}
	//attempt to add with a bad id or empty password
	if err = a.AddUser(1234, ``, 10); err == nil {
		t.Fatal("failed to catch bad password")
	}
	if err = a.AddUser(0, `password`, 10); err == nil {
		t.Fatal("failed to catch bad id")
	}
	//query our new user
	if cid, err := a.Authenticate(`10`, `password`); err != nil {
		t.Fatal(err)
	} else if cid != 10 {
		t.Fatal("Bad CID")
	}
}

func TestChange(t *testing.T) {
	pth := filepath.Join(tdir, "test5")
	//test with some existing data
	if err := dropTestFile(pth); err != nil {
		t.Fatal(err)
	}
	a, err := NewAuthModule(pth)
	if err != nil {
		t.Fatal(err)
	}
	//check an auth real quick
	if cid, err := a.Authenticate(testUser1IDS, testUser1Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser1ID {
		t.Fatal("bad userid")
	}
	//add a new user
	if err = a.AddUser(10, `password`, 10); err != nil {
		t.Fatal(err)
	}
	//query our new user
	if cid, err := a.Authenticate(`10`, `password`); err != nil {
		t.Fatal(err)
	} else if cid != 10 {
		t.Fatal("Bad CID")
	}
	//change the new users password
	if err := a.ChangePassword(10, `password1`); err != nil {
		t.Fatal(err)
	}
	//check authentication with old passowrd
	if _, err := a.Authenticate(`10`, `password`); err == nil {
		t.Fatal("failed to change password")
	}
	//check authentication with new password
	if cid, err := a.Authenticate(`10`, `password1`); err != nil {
		t.Fatal(err)
	} else if cid != 10 {
		t.Fatal("Bad CID")
	}
}

func TestRemove(t *testing.T) {
	pth := filepath.Join(tdir, "test6")
	//test with some existing data
	if err := dropTestFile(pth); err != nil {
		t.Fatal(err)
	}
	a, err := NewAuthModule(pth)
	if err != nil {
		t.Fatal(err)
	}
	//check an auth real quick
	if cid, err := a.Authenticate(testUser1IDS, testUser1Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser1ID {
		t.Fatal("bad userid")
	}
	if err = a.DeleteUser(testUser1ID); err != nil {
		t.Fatal(err)
	}
	//check that we can't authenticate
	if _, err := a.Authenticate(testUser1IDS, testUser1Password); err != ErrInvalidUser {
		t.Fatal("user still exists")
	}
	//check that we still CAN authenticate other users
	if cid, err := a.Authenticate(testUser2IDS, testUser2Password); err != nil {
		t.Fatal(err)
	} else if cid != testUser2ID {
		t.Fatal("bad userid")
	}

}

func dropTestFile(p string) error {
	fout, err := os.Create(p)
	if err != nil {
		return err
	}
	if _, err = io.WriteString(fout, testUser1+"\n"); err != nil {
		fout.Close()
		return err
	}
	if _, err = io.WriteString(fout, testUser2+"\n"); err != nil {
		fout.Close()
		return err
	}
	if err = fout.Close(); err != nil {
		return err
	}
	return nil
}
