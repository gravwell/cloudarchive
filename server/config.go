/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gravwell/gcfg"
	icfg "github.com/gravwell/gravwell/v3/ingest/config"
	"golang.org/x/sys/unix"
)

const (
	MAX_CONFIG_SIZE   int64  = (1024 * 1024 * 2) //2MB, even this is crazy large
	defaultListenPort uint16 = 443
)

type cfgType struct {
	Global struct {
		Listen_Address    string
		Cert_File         string
		Key_File          string
		Password_File     string
		Log_File          string
		Log_Level         string
		Storage_Directory string
	}
}

func GetConfig(path string) (*cfgType, error) {
	var content []byte
	fin, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := fin.Stat()
	if err != nil {
		fin.Close()
		return nil, err
	}
	//This is just a sanity check
	if fi.Size() > MAX_CONFIG_SIZE {
		fin.Close()
		return nil, errors.New("Config File far too large")
	}
	content = make([]byte, fi.Size())
	n, err := fin.Read(content)
	fin.Close()
	if int64(n) != fi.Size() {
		return nil, errors.New("Failed to read config file")
	}

	var c cfgType
	if err := gcfg.ReadStringInto(&c, string(content)); err != nil {
		return nil, err
	}
	if err := verifyConfig(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func verifyConfig(c *cfgType) error {
	if c.Global.Cert_File == `` {
		return errors.New("Must specify Cert-File")
	}
	if c.Global.Key_File == `` {
		return errors.New("Must specify Key-File")
	}
	if c.Global.Password_File == `` {
		return errors.New("Must specify Password-File")
	}
	if c.Global.Storage_Directory == `` {
		return errors.New("Must specify Storage-Directory")
	} else if err := writableDir(c.Global.Storage_Directory); err != nil {
		return fmt.Errorf("Storage-Directory error %v", err)
	}
	if c.Global.Listen_Address == `` {
		return fmt.Errorf("Listen-Address is empty")
	} else {
		//potentially append the default port
		c.Global.Listen_Address = icfg.AppendDefaultPort(c.Global.Listen_Address, defaultListenPort)
	}
	ll := strings.ToUpper(strings.TrimSpace(c.Global.Log_Level))
	switch ll {
	case `INFO`:
	case `WARN`:
	case `ERROR`:
	case `OFF`:
	case ``:
		// if nothing is specified, we default to WARN
		c.Global.Log_Level = `WARN`
	default:
		return fmt.Errorf("%s is an invalid log level", c.Global.Log_Level)
	}
	return nil
}

// writableDir ensures that the provided location exists, is a dir, and is R/W
func writableDir(pth string) error {
	if fi, err := os.Stat(pth); err != nil {
		return err
	} else if !fi.Mode().IsDir() {
		return errors.New("not a directory")
	} else if err = unix.Access(pth, unix.R_OK); err != nil {
		return err
	} else if err = unix.Access(pth, unix.W_OK); err != nil {
		return err
	}
	return nil
}
