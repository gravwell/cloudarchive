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
	icfg "github.com/gravwell/gravwell/v4/ingest/config"
	"golang.org/x/sys/unix"
)

const (
	MAX_CONFIG_SIZE   int64  = (1024 * 1024 * 2) //2MB, even this is crazy large
	defaultListenPort uint16 = 443

	BackendTypeFTP  = "ftp"
	BackendTypeFile = "file"

	DefaultBackendType = BackendTypeFile
)

type cfgType struct {
	Global struct {
		Listen_Address string
		Disable_TLS    bool
		Cert_File      string
		Key_File       string
		Password_File  string
		Log_File       string
		Log_Level      string

		// Select the storage backend
		Backend_Type string
		// Storage-Directory is used by file *and* ftp, because the FTP backend
		// also needs a place to stage some files.
		Storage_Directory string
		// File backend options
		// (currently no file-specific options)
		// FTP backend options
		FTP_Server            string // addr:port
		Remote_Base_Directory string // the base directory on the FTP server to use, if the default dir isn't acceptable
		FTP_Username          string
		FTP_Password          string
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
	if err != nil {
		return nil, err
	}
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
	if !c.Global.Disable_TLS {
		if c.Global.Cert_File == `` {
			return errors.New("Must specify Cert-File")
		}
		if c.Global.Key_File == `` {
			return errors.New("Must specify Key-File")
		}
	}
	if c.Global.Password_File == `` {
		return errors.New("Must specify Password-File")
	}

	// Figure out what kind of backend we're going to use
	if c.Global.Backend_Type == `` {
		c.Global.Backend_Type = DefaultBackendType
	}
	if c.Global.Storage_Directory == `` {
		return errors.New("Must specify Storage-Directory")
	} else if err := writableDir(c.Global.Storage_Directory); err != nil {
		return fmt.Errorf("Storage-Directory error %v", err)
	}
	switch c.Global.Backend_Type {
	case BackendTypeFile:
	case BackendTypeFTP:
		if c.Global.FTP_Server == `` {
			return errors.New("Must specify FTP-Server")
		} else if c.Global.FTP_Username == `` {
			return errors.New("Must specify FTP-Username")
		} else if c.Global.FTP_Password == `` {
			return errors.New("Must specify FTP-Password")
		}
		// it's ok to leave Remote-Base-Directory empty.
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
