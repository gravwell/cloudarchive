/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"bytes"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"os"

	"github.com/gravwell/cloudarchive/pkg/configbuilder"
)

var (
	fStub = flag.String("stub", "", "Path to config file stub")
	fDir  = flag.String("dir", "", "Base directory for new config, should be the top-level dir of an indexer (final component is a UUID, e.g. /var/archives/<custid>/<indexeruuid>")
	fOut  = flag.String("o", "", "Output file for configuration.  STDOUT if empty")
)

func init() {
	flag.Parse()
	if *fStub == `` {
		log.Fatal("config stub required")
	} else if *fDir == `` {
		log.Fatal("directory is required")
	}
}

func main() {
	// Read the file
	stubContents, err := ioutil.ReadFile(*fStub)
	if err != nil {
		log.Fatalf("Couldn't read config file stub: %v", err)
	}

	result, err := configbuilder.BuildConfig(stubContents, *fDir)
	if err != nil {
		log.Fatalf("Couldn't build config: %v", err)
	}

	var out io.Writer
	if *fOut != `` {
		fout, err := os.Create(*fOut)
		if err != nil {
			log.Fatalf("Failed to create output file %s: %v\n", *fOut, err)
		}
		out = fout
		defer fout.Close()
	} else {
		out = os.Stdout
	}
	if n, err := io.Copy(out, bytes.NewBuffer(result)); err != nil {
		log.Fatal(err)
	} else if n != int64(len(result)) {
		log.Fatal("Failed to write entire config file")
	}
}
