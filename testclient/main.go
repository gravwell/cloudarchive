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
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/gravwell/cloudarchive/pkg/client"
	"github.com/gravwell/cloudarchive/pkg/tags"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v4/ingest/log"
	"github.com/manifoldco/promptui"
)

var (
	fCustID   = flag.String("id", "17", "customer id")
	fPassword = flag.String("password", "foo", "password")
	fServer   = flag.String("s", "localhost:8888", "server url")
	fTags     = flag.String("tags", "", "path to tags.dat")
	fUUID     = flag.String("uuid", "", "UUID override")
	fWell     = flag.String("well", "", "Well name override")
	fShard    = flag.String("shard", "", "shard name override")
	fNossl    = flag.Bool("nossl", false, "Use an insecure HTTP connection")
	guid      = uuid.New()
	cmd       string
	args      []string
)

func init() {
	flag.Parse()
	if *fCustID == `` || *fPassword == `` || *fServer == `` || *fTags == `` {
		fmt.Fprintf(os.Stderr, "Missing flags\n")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	if *fUUID != `` {
		var err error
		if guid, err = uuid.Parse(*fUUID); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid UUID override: %v\n", err)
			os.Exit(-1)
		}
	}
	x := flag.Args()
	if len(x) > 0 {
		cmd = x[0]
		if len(x) > 1 {
			args = x[1:]
		}
	}
}

func main() {
	lgr := log.New(os.Stderr)

	cli, err := client.NewClient(*fServer, false, !*fNossl)
	if err != nil {
		lgr.Fatalf("%v", err)
	}

	if err = cli.Test(); err != nil {
		lgr.Fatalf("%v", err)
	}

	if err = cli.Login(*fCustID, *fPassword); err != nil {
		lgr.Fatalf("%v", err)
	}

	if err = cli.TestLogin(); err != nil {
		lgr.Fatalf("%v", err)
	}

	tm, err := tags.New(*fTags)
	if err != nil {
		lgr.Fatalf("%v", err)
	}
	if err = runSession(cli, tm, lgr); err != nil {
		tm.Close()
		lgr.Fatalf("session failure: %v", err)
	} else if err = tm.Close(); err != nil {
		lgr.Fatalf("Failed to close tag manager: %v", err)
	}
}

func runSession(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	if cmd != `` {
		err = runStaticSession(cli, tm, lgr)
		return
	}
	prompt := promptui.Select{
		Label: "Select Operation",
		Items: []string{pushShard, pullTags, syncTags, listIndexers, listIndexerWells, getWellTimeframe, getWellShards, pullShard, `exit`},
	}
	var op string
	if _, op, err = prompt.Run(); err != nil {
		return
	}
	switch op {
	case pushShard:
		err = PushShard(cli, tm, lgr)
	case pullTags:
		err = PullTags(cli, tm, lgr)
	case syncTags:
		err = SyncTags(cli, tm, lgr)
	case listIndexers:
		err = ListKnownIndexers(cli, tm, lgr)
	case listIndexerWells:
		err = ListIndexerWells(cli, tm, lgr)
	case getWellTimeframe:
		err = GetWellTimeframe(cli, tm, lgr)
	case getWellShards:
		err = GetWellShards(cli, tm, lgr)
	case pullShard:
		err = PullShard(cli, tm, lgr)
	case `exit`:
	default:
		err = errors.New("Unknown operation")
	}
	return
}

var (
	staticPushShard    string = `push`
	staticPullShard    string = `pull`
	staticSyncTags     string = `synctags`
	staticPullTags     string = `tags`
	staticListIdxs     string = `indexes`
	staticListWells    string = `wells`
	staticListShards   string = `shards`
	staticListWellTime string = `welltime`
)

func runStaticSession(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	switch strings.ToLower(cmd) {
	case `help`:
		printCommands()
	case staticPushShard:
		err = PushShard(cli, tm, lgr)
	case staticPullShard:
		err = PullShard(cli, tm, lgr)
	case staticPullTags:
		err = PullTags(cli, tm, lgr)
	case staticSyncTags:
		err = SyncTags(cli, tm, lgr)
	case staticListIdxs:
		err = ListKnownIndexers(cli, tm, lgr)
	case staticListWells:
		err = ListIndexerWells(cli, tm, lgr)
	case staticListShards:
		err = GetWellShards(cli, tm, lgr)
	case staticListWellTime:
		err = GetWellTimeframe(cli, tm, lgr)
	}
	return
}

func printCommands() {
	fmt.Println("Options are:")
	fmt.Printf("\t%s <shard path>\n", staticPushShard)
	fmt.Printf("\t%s <store path>\n", staticPullShard)
	fmt.Printf("\t%s\n", staticPullTags)
	fmt.Printf("\t%s\n", staticListIdxs)
	fmt.Printf("\t%s\n", staticListWells)
	fmt.Printf("\t%s\n", staticListShards)
	fmt.Printf("\t%s\n", staticListWellTime)
}
