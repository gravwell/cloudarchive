/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package auth

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/gravwell/cloudarchive/pkg/flock"

	"golang.org/x/crypto/bcrypt"
)

const (
	defaultCost   int    = 12 //bcrypt cost
	minCost       int    = 8  //not passwords may be below this cost
	lineSplitChar string = `:`
)

var (
	ErrNotOpen         = errors.New("AuthModule not ready")
	ErrNotFound        = errors.New("user id not found")
	ErrEmptyLine       = errors.New("Empty password line")
	ErrInvalidHashCost = errors.New("password line has an invalid hash cost")
	ErrCorruptLine     = errors.New("passwd line is corrupt")
	ErrInvalidUser     = errors.New("Invalid user")
	ErrCustnumExists   = errors.New("userid already exists")
)

type userHash struct {
	custnum uint64
	hash    []byte
}

type Auth struct {
	sync.Mutex
	fpath string
}

func NewAuthModule(fpath string) (*Auth, error) {
	//validate that the file exists and is a regular file
	if fi, err := os.Stat(fpath); err != nil {
		if os.IsNotExist(err) {
			if err = testFile(fpath); err != nil {
				return nil, err
			}
			//we were able to create the file
			return &Auth{fpath: fpath}, nil
		}
		//some other error
		return nil, err
	} else if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", fpath)
	}

	//open it to ensure we can read and write from the file
	if err := testFile(fpath); err != nil {
		return nil, err
	}
	//file exists and we can read and write from it
	return &Auth{fpath: fpath}, nil
}

// List returns a list of current users
func (a *Auth) List() (uhs []userHash, err error) {
	a.Lock()
	uhs, err = a.load()
	a.Unlock()
	return
}

// load opens the file, locks it, loads the contents and closes it
func (a *Auth) load() (uhs []userHash, err error) {
	var fin *os.File
	var uh userHash
	if a.fpath == `` {
		err = ErrNotOpen
		return
	}
	if fin, err = os.OpenFile(a.fpath, os.O_RDWR, 0660); err != nil {
		return
	}
	//get an exclusive lock on the file
	if err = flock.Flock(fin, true); err != nil {
		fin.Close()
		return
	}

	scn := bufio.NewScanner(fin)
	for scn.Scan() {
		if err = uh.Parse(scn.Text()); err != nil {
			flock.Funlock(fin)
			fin.Close()
			return
		}
		uhs = append(uhs, uh)
	}
	//check the scanner for errors
	if err = scn.Err(); err != nil {
		flock.Funlock(fin)
		fin.Close()
		return
	}

	//unlock the file
	if err = flock.Funlock(fin); err != nil {
		fin.Close()
		return
	}

	//close and return any potential errors
	err = fin.Close()
	return
}

func (a *Auth) Authenticate(custnum, passwd string) (cid uint64, err error) {
	var uhs []userHash
	if len(custnum) == 0 || len(passwd) == 0 {
		err = errors.New("empty auth parameters")
		return
	}
	if cid, err = strconv.ParseUint(custnum, 10, 64); err != nil {
		return
	}
	a.Lock()
	uhs, err = a.load()
	a.Unlock()
	if err != nil {
		return
	}
	for _, uh := range uhs {
		if uh.custnum == cid {
			err = bcrypt.CompareHashAndPassword(uh.hash, []byte(passwd))
			return
		}
	}
	err = ErrInvalidUser
	return
}

func (a *Auth) AddUser(custnum uint64, passwd string, cost int) (err error) {
	var uhs []userHash
	if cost > bcrypt.MaxCost {
		cost = bcrypt.MaxCost
	} else if cost < minCost {
		cost = minCost
	}
	if custnum == 0 || len(passwd) == 0 {
		err = errors.New("empty auth parameters")
		return
	}
	a.Lock()
	defer a.Unlock()
	if uhs, err = a.load(); err != nil {
		return
	}

	for _, uh := range uhs {
		if uh.custnum == custnum {
			err = ErrCustnumExists
			return
		}
	}

	//this is a new customer, encode and append
	uh := userHash{custnum: custnum}
	if uh.hash, err = bcrypt.GenerateFromPassword([]byte(passwd), cost); err != nil {
		return
	}
	err = a.addUser(uh)
	return
}

func (a *Auth) DeleteUser(custnum uint64) (err error) {
	var uhs []userHash
	if custnum == 0 {
		err = errors.New("empty auth parameters")
		return
	}
	a.Lock()
	defer a.Unlock()
	if uhs, err = a.load(); err != nil {
		return
	}
	idx := -1
	for i, u := range uhs {
		if u.custnum == custnum {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ErrNotFound
	}
	uhs = append(uhs[:idx], uhs[idx+1:]...)
	err = a.updateUsers(uhs)
	return
}

func (a *Auth) ChangePassword(custnum uint64, passwd string) (err error) {
	var uhs []userHash
	if custnum == 0 || len(passwd) == 0 {
		err = errors.New("empty auth parameters")
		return
	}
	a.Lock()
	defer a.Unlock()
	if uhs, err = a.load(); err != nil {
		return
	}
	idx := -1
	for i, u := range uhs {
		if u.custnum == custnum {
			idx = i
			break
		}
	}
	if idx == -1 {
		return ErrNotFound
	}
	//get the existing cost
	var cost int
	if cost, err = bcrypt.Cost(uhs[idx].hash); err != nil {
		return
	}
	//check and update the cost
	if cost > bcrypt.MaxCost {
		cost = bcrypt.MaxCost
	} else if cost < minCost {
		cost = minCost
	}

	//encode and update
	if uhs[idx].hash, err = bcrypt.GenerateFromPassword([]byte(passwd), cost); err != nil {
		return
	}
	err = a.updateUsers(uhs)
	return
}

// updateUsers updates the entire file, the caller must hold the lock
func (a *Auth) updateUsers(uhs []userHash) (err error) {
	pth := a.fpath + ".tmp"
	if a.fpath == `` {
		err = ErrNotOpen
		return
	}
	//open our new file
	var fn *os.File
	if fn, err = os.OpenFile(pth, os.O_RDWR|os.O_CREATE, 0660); err != nil {
		return
	}
	//get an exclusive lock on the file
	if err = flock.Flock(fn, true); err != nil {
		fn.Close()
		return
	}

	//write out our users
	for _, uh := range uhs {
		if _, err = fmt.Fprintf(fn, "%d:%s\n", uh.custnum, string(uh.hash)); err != nil {
			flock.Funlock(fn)
			fn.Close()
			os.Remove(pth)
			return
		}
	}

	//open the exising file and lock it
	var fio *os.File
	if fio, err = os.OpenFile(a.fpath, os.O_RDWR, 0660); err != nil {
		flock.Funlock(fn)
		fn.Close()
		os.Remove(pth)
		return
	}
	//get an exclusive lock on the file
	if err = flock.Flock(fio, true); err != nil {
		flock.Funlock(fn)
		fn.Close()
		os.Remove(pth)
		fio.Close()
		return
	}

	//rename/overwrite
	if err = os.Rename(pth, a.fpath); err != nil {
		flock.Funlock(fn)
		flock.Funlock(fio)
		fn.Close()
		fio.Close()
		os.Remove(pth)
		return
	}
	//overwrite is done, clean up temp file
	if err = flock.Funlock(fn); err != nil {
		flock.Funlock(fio)
		fn.Close()
		fio.Close()
		return

	}
	if err = fn.Close(); err != nil {
		flock.Funlock(fio)
		fio.Close()
		return
	}
	if err = flock.Funlock(fio); err != nil {
		fio.Close()
		return
	}
	err = fio.Close()
	return
}

// addUser appends a user to the file, the caller must hold the lock
func (a *Auth) addUser(uh userHash) (err error) {
	var fio *os.File
	if a.fpath == `` {
		err = ErrNotOpen
		return
	}
	if fio, err = os.OpenFile(a.fpath, os.O_RDWR|os.O_APPEND, 0660); err != nil {
		return
	}
	//get an exclusive lock on the file
	if err = flock.Flock(fio, true); err != nil {
		fio.Close()
		return
	}

	if _, err = fmt.Fprintf(fio, "%d:%s\n", uh.custnum, string(uh.hash)); err != nil {
		flock.Funlock(fio)
		fio.Close()
		return
	}

	//unlock the file
	if err = flock.Funlock(fio); err != nil {
		fio.Close()
		return
	}

	//close and return any potential errors
	err = fio.Close()
	return
}

// Parse takes an input line and generates the userid and bcrypt hash components
// the components are assigned to the user hash
func (uh *userHash) Parse(v string) error {
	v = strings.Trim(v, "\n\t ") //trim any newlines, spaces, and tabs
	if len(v) == 0 {
		return ErrEmptyLine
	}

	//crack the line into its two components
	bits := strings.Split(v, lineSplitChar)
	if len(bits) != 2 {
		return ErrCorruptLine
	}

	//parse the userid component
	var err error
	if uh.custnum, err = strconv.ParseUint(bits[0], 10, 64); err != nil {
		return fmt.Errorf("Invalid customer number %s: %v", bits[0], err)
	}
	uh.hash = []byte(bits[1])
	var cost int
	if cost, err = bcrypt.Cost(uh.hash); err != nil {
		return err
	} else if cost < minCost {
		return ErrInvalidHashCost
	}
	//successful parse
	return nil
}

func (uh *userHash) ID() uint64 {
	return uh.custnum
}

func testFile(p string) error {
	if f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0660); err != nil {
		return err
	} else if err = f.Close(); err != nil {
		return err
	}
	return nil
}
