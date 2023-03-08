/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/gravwell/cloudarchive/pkg/auth"

	"github.com/howeyc/gopass"
)

const (
	passCost int = 12
)

var (
	fpath = flag.String("passfile", "", "Path to the password file")
	fact  = flag.String("action", "list", "action to take (list, useradd, userdel, passwd)")
	fuid  = flag.Uint("id", 0, "User ID")
	fpwd  = flag.String("password", "", "Password to use when adding a user, if blank you will be prompted")
)

func init() {
	flag.Parse()
	if *fpath == `` {
		log.Fatal("passfile path is required")
	} else if err := checkAction(*fact); err != nil {
		log.Fatalf("action %s is invalid: %v\n", *fact, err)
	}
}

func main() {
	am, err := auth.NewAuthModule(*fpath)
	if err != nil {
		log.Fatalf("Failed to initialize auth module: %v\n", err)
	}
	switch *fact {
	case `list`:
		listUsers(am)
	case `useradd`:
		addUser(am, uint64(*fuid))
	case `userdel`:
		delUser(am, uint64(*fuid))
	case `passwd`:
		chpasswd(am, uint64(*fuid))
	}
}

func listUsers(am *auth.Auth) {
	uhs, err := am.List()
	if err != nil {
		log.Fatalf("Failed to get user list: %v\n", err)
	} else if len(uhs) == 0 {
		fmt.Println("No users")
		return
	}
	for _, uh := range uhs {
		fmt.Println(uh.ID())
	}
}

func delUser(am *auth.Auth, id uint64) {
	if err := am.DeleteUser(id); err != nil {
		log.Fatalf("Failed to delete id %d: %v\n", id, err)
	}
	fmt.Printf("ID %d deleted\n", id)
}

func addUser(am *auth.Auth, id uint64) {
	var err error
	pass := []byte(*fpwd)
	if len(pass) == 0 {
		fmt.Printf("Enter %d passphrase: ", id)
		if pass, err = gopass.GetPasswd(); err != nil {
			log.Fatalf("Failed to get passphrase for %d\n", id)
		}
	}
	if err = am.AddUser(id, string(pass), passCost); err != nil {
		log.Fatalf("Failed to add id %d: %v\n", id, err)
	}
	fmt.Printf("ID %d added\n", id)
}

func chpasswd(am *auth.Auth, id uint64) {
	fmt.Printf("Enter %d passphrase: ", id)
	pass, err := gopass.GetPasswd()
	if err != nil {
		log.Fatalf("Failed to get passphrase for %d\n", id)
	}
	if err = am.ChangePassword(id, string(pass)); err != nil {
		log.Fatalf("Failed to change passphrase for id %d: %v\n", id, err)
	}
	fmt.Printf("ID %d passphrase changed\n", id)
}

func checkAction(act string) (err error) {
	switch act {
	case `list`:
	case `useradd`:
		fallthrough
	case `userdel`:
		fallthrough
	case `passwd`:
		if *fuid == 0 {
			err = fmt.Errorf("Action %s requires a user id", act)
		}
	default:
		err = fmt.Errorf("%s is an invalid action", act)
	}
	return
}
