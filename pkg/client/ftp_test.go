/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package client

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/gravwell/cloudarchive/pkg/auth"
	"github.com/gravwell/cloudarchive/pkg/ftpstore"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/webserver"

	"github.com/gravwell/gravwell/v3/ingest/entry"
	gravlog "github.com/gravwell/gravwell/v3/ingest/log"
)

const ()

var ()

func launchWebserverFTP() error {
	var err error
	lgr := gravlog.New(discarder{})

	cfg := ftpstore.FtpStoreConfig{
		BaseDir:    "testing",
		LocalStore: localStoreDir,
		FtpServer:  "127.0.0.1:2000",
		Username:   "gravwell",
		Password:   "testpass",
	}
	handler, err := ftpstore.NewFtpStoreHandler(cfg)
	if err != nil {
		return err
	}

	conf := webserver.WebserverConfig{
		ListenString: listenAddr,
		CertFile:     certFile,
		KeyFile:      keyFile,
		Logger:       lgr,
		ShardHandler: handler,
	}
	if conf.Auth, err = auth.NewAuthModule(passwordFile); err != nil {
		return err
	}

	ws, err = webserver.NewWebserver(conf)
	if err != nil {
		return err
	}

	err = ws.Init()
	if err != nil {
		return err
	}

	err = ws.Run()
	if err != nil {
		return err
	}
	return nil
}

func TestFtpClientConnect(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Test()
	if err != nil {
		t.Fatal(err)
	}

	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientLogin(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Test()
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.TestLogin()
	if err != nil {
		t.Fatal(err)
	}

	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientShardPush(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	shardid := `76d71`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}
	tps := []tags.TagPair{
		tags.TagPair{Name: `testing`, Value: 1},
	}
	tags := []string{`testing`}
	cancel := make(chan bool, 1)

	//make a fake shard dir with the
	sdir := filepath.Join(baseDir, shardid)
	if err = makeShardDir(sdir, shardid); err != nil {
		t.Fatal(err)
	}
	if err = cli.PushShard(sid, sdir, tps, tags, cancel); err != nil {
		t.Fatal(err)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientShardPull(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	shardid := `76d71`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}

	sdir := filepath.Join(baseDir, "pull", shardid)
	cancel := make(chan bool, 1)
	if err = cli.PullShard(sid, sdir, cancel); err != nil {
		t.Fatal(err)
	}

	if err := validateShardExists(sdir, shardid); err != nil {
		t.Fatal(err)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientListIndexers(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	// Push a shard
	shardid := `769fd`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}
	tps := []tags.TagPair{
		tags.TagPair{Name: `testing`, Value: 1},
	}
	tags := []string{`testing`}
	cancel := make(chan bool, 1)

	//make a fake shard dir with the
	sdir := filepath.Join(baseDir, shardid)
	if err = makeShardDir(sdir, shardid); err != nil {
		t.Fatal(err)
	}
	if err = cli.PushShard(sid, sdir, tps, tags, cancel); err != nil {
		t.Fatal(err)
	}

	indexers, err := cli.ListIndexers()
	if err != nil {
		t.Fatal(err)
	}
	if len(indexers) != 1 {
		t.Fatalf("Invalid number of indexers: got %v expected %v", len(indexers), 1)
	}
	if indexers[0] != idxUUID.String() {
		t.Fatalf("Invalid indexer: got %v expected %v", indexers[0], idxUUID.String())
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientListIndexerWells(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	wells, err := cli.ListIndexerWells(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(wells) != 1 {
		t.Fatalf("Invalid number of wells: got %v expected %v", len(wells), 1)
	}
	if wells[0] != `foo` {
		t.Fatalf("Invalid well name, got %v expected foo", wells[0])
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientGetTimeframe(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	tf, err := cli.GetWellTimeframe(idxUUID.String(), "foo")
	// Ask for the time frame covered by the well "foo"
	if err != nil {
		t.Fatal(err)
	}
	if tf.Start.IsZero() || tf.End.IsZero() {
		t.Fatalf("Got a zero in the timeframe: %v", tf)
	}
	if tf.Start.After(tf.End) {
		t.Fatalf("uhh, end is before start?")
	}

	// Get the list of shards covered by that time frame
	shards, err := cli.GetWellShardsInTimeframe(idxUUID.String(), "foo", tf)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 {
		t.Fatalf("Expected 2 shards, got %d", len(shards))
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientPullTags(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	tagset, err := cli.PullTags(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(tagset) != 3 {
		t.Fatalf("Invalid tagset, expected 3 tags got: %+v\n", tagset)
	}
	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFtpClientSyncTags(t *testing.T) {
	// Start a webserver
	if err := launchWebserverFTP(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	// Pull the tags
	tagset, err := cli.PullTags(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(tagset) != 3 {
		t.Fatalf("Invalid tagset, expected 3 tags got: %+v\n", tagset)
	}

	// Now add a new tag
	tagset = append(tagset, tags.TagPair{Name: "xyzzy", Value: entry.EntryTag(100)})
	if newset, err := cli.SyncTags(idxUUID.String(), tagset); err != nil {
		t.Fatal(err)
	} else {
		if len(newset) != 4 {
			t.Fatalf("Expected 4 tags in new tag set, got %v", len(newset))
		}
		var ok bool
		for _, t := range newset {
			if t.Name == "xyzzy" {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("Did not find newly-added tag in tag set %v", newset)
		}
	}
	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}
