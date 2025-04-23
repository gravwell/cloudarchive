/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package configbuilder

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
)

const (
	baseShard = 0x76a00
)

var (
	stubConfig = []byte(`[Global]
Indexer-UUID=5f8fe13a-4033-11e9-8c1c-54e1ad7c66cf
Webserver-UUID=6677cb66-4033-11e9-9d35-54e1ad7c66cf
Ingest-Port=4023
`)

	wells = []wellInfo{
		{`default`, []string{}},
		{`syslog`, []string{"syslog"}},
		{`raw`, []string{"pcap", "video"}},
		{`bro`, []string{"bro-conn", "bro-dns", "bro-http"}},
	}
)

type wellInfo struct {
	name string
	tags []string
}

func makeTestDirs() (baseDir string, uuidPath string, err error) {
	baseDir, err = os.MkdirTemp(os.TempDir(), "configbuilder")
	if err != nil {
		return
	}

	// make a UUID dir
	id := uuid.New()
	uuidPath = filepath.Join(baseDir, fmt.Sprintf("%v", id))
	if err = os.Mkdir(uuidPath, 0777); err != nil {
		os.RemoveAll(baseDir)
		return
	}

	// make some wells
	for _, well := range wells {
		wellPath := filepath.Join(uuidPath, well.name)
		if err = os.Mkdir(wellPath, 0777); err != nil {
			os.RemoveAll(baseDir)
			return
		}

		var tagList string
		for _, t := range well.tags {
			tagList = fmt.Sprintf("%s%s\n", tagList, t)
		}

		// make shards in that well
		count := int(rand.Int31n(5) + 1)
		for i := 0; i < count; i++ {
			shardPath := filepath.Join(wellPath, fmt.Sprintf("%x", baseShard+i))
			if err = os.Mkdir(shardPath, 0777); err != nil {
				os.RemoveAll(baseDir)
				return
			}
			// Drop in the tags file
			if err = os.WriteFile(filepath.Join(shardPath, "tags"), []byte(tagList), 0777); err != nil {
				os.RemoveAll(baseDir)
				return
			}
		}
	}

	return
}

func TestBuildConfig(t *testing.T) {
	baseDir, uuidDir, err := makeTestDirs()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(baseDir)

	if conf, err := BuildConfig(stubConfig, uuidDir); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(string(conf))
	}
}
