/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

// Package configbuilder implements all the configuration management and builder functions for the Gravwell CloudArchive system
package configbuilder

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func BuildConfig(stub []byte, baseDir string) (conf []byte, err error) {
	// Extract the UUID from the basedir, better be the last component
	indexerUUID, err := uuid.Parse(filepath.Base(baseDir))
	if err != nil {
		return
	}

	// Attempt to find and replace the Indexer-UUID field
	re := regexp.MustCompile(`(Indexer-UUID=).+`)
	match := re.Find(stub)
	if match == nil {
		err = errors.New("No Indexer-UUID field in stub config")
		return
	}
	conf = re.ReplaceAll(stub, []byte(fmt.Sprintf("${1}%v", indexerUUID)))

	// Now, walk the basedir
	var wells []os.DirEntry
	wells, err = os.ReadDir(baseDir)
	if err != nil {
		return
	}

	// For each directory:
	for _, well := range wells {
		// If it's a directory, we assume it's a well, otherwise skip
		if !well.IsDir() {
			continue
		}
		p := filepath.Join(baseDir, well.Name())

		// Now list that directory
		var shards []os.DirEntry
		shards, err = os.ReadDir(p)
		if err != nil {
			return
		}

		// Find the most recent shard
		var newest uint64
		var newestName string
		for _, shard := range shards {
			if !shard.IsDir() {
				continue
			}
			// parse the name as a hex number
			u, err := strconv.ParseUint(shard.Name(), 16, 64)
			if err != nil {
				continue
			}
			if u > newest {
				newest = u
				newestName = shard.Name()
			}
		}

		if newest == 0 {
			// we didn't find any shards, skip
			continue
		}

		newestPath := filepath.Join(p, newestName)

		// Read its 'tags' file (unless default well)
		var tagList []string
		if well.Name() != `default` {
			var tagContents []byte
			tagContents, err = os.ReadFile(filepath.Join(newestPath, "tags"))
			if err != nil {
				return
			}
			// Split the contents based on newline
			parts := strings.Split(string(tagContents), "\n")
			if len(parts) == 0 {
				// all non-default wells must specify tags!
				err = fmt.Errorf("No tags found in %v!", newestPath)
				return
			}
			for _, p := range parts {
				if p != `` {
					tagList = append(tagList, p)
				}
			}
		}

		// Construct a Well entry based on that, assuming baseDir will map to /opt/gravwell/storage
		var wellEntry string
		if well.Name() == `default` {
			wellEntry = "[Default-Well]\n"
		} else {
			wellEntry = fmt.Sprintf("[Storage-Well \"%v\"]\n", well.Name())
		}
		wellEntry = fmt.Sprintf("%s	Location=%s/%s\n", wellEntry, baseDir, well.Name())
		for _, t := range tagList {
			wellEntry = fmt.Sprintf("%s	Tags=%s\n", wellEntry, t)
		}

		// Append the well to the config
		conf = bytes.Join([][]byte{conf, []byte(wellEntry)}, []byte{})
	}
	return
}
