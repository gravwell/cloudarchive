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
	"os"
	"path/filepath"
	"strconv"

	"github.com/gravwell/cloudarchive/pkg/client"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/manifoldco/promptui"
)

const (
	pullShard        string = `Pull Shard`
	pullTags         string = `Pull Tags`
	syncTags         string = `Sync Tags`
	pushShard        string = `Push Shard`
	listIndexers     string = `List Indexers`
	listIndexerWells string = `List Indexer Wells`
	getWellTimeframe string = `Get Well Timeframe`
	getWellShards    string = `Get Well Shards`
)

func PullTags(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var tset []tags.TagPair
	if tset, err = cli.PullTags(guid.String()); err == nil {
		_, err = tm.Merge(tset)
	}
	return
}

func SyncTags(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var tset []tags.TagPair
	if tset, err = tm.TagSet(); err != nil {
		return
	}
	_, err = cli.SyncTags(guid.String(), tset)
	return
}

func ListKnownIndexers(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var idx []string
	if idx, err = cli.ListIndexers(); err != nil {
		return
	}
	lgr.Info("Indexers:")
	for i := range idx {
		lgr.Info(idx[i])
	}
	return
}

func getIndexer(cli *client.Client) (indexer string, err error) {
	if indexer = *fUUID; indexer == `` {
		var idx []string
		if idx, err = cli.ListIndexers(); err != nil {
			return
		}
		prompt := promptui.Select{
			Label: "Select Indexer",
			Items: idx,
		}
		if _, indexer, err = prompt.Run(); err != nil {
			return
		}
	}
	return
}

func ListIndexerWells(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var indexer string
	if indexer, err = getIndexer(cli); err != nil {
		return
	}
	var wells []string
	if wells, err = cli.ListIndexerWells(indexer); err != nil {
		return
	}
	lgr.Infof("Wells on indexer %v:", indexer)
	for i := range wells {
		lgr.Info(wells[i])
	}
	return
}

func getWell(cli *client.Client, indexer string) (well string, err error) {
	if well = *fWell; well == `` {
		var wells []string
		if wells, err = cli.ListIndexerWells(indexer); err != nil {
			return
		}
		prompt := promptui.Select{
			Label: "Select Well",
			Items: wells,
		}
		if _, well, err = prompt.Run(); err != nil {
			return
		}
	}
	return
}

func GetWellTimeframe(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var indexer, well string
	if indexer, err = getIndexer(cli); err != nil {
		return
	}
	if well, err = getWell(cli, indexer); err != nil {
		return
	}
	var tf util.Timeframe
	if tf, err = cli.GetWellTimeframe(indexer, well); err != nil {
		return
	}

	lgr.Infof("Well data starts at %v and ends at %v", tf.Start, tf.End)
	return
}

func GetWellShards(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var indexer, well string
	if indexer, err = getIndexer(cli); err != nil {
		return
	}
	if well, err = getWell(cli, indexer); err != nil {
		return
	}

	var tf util.Timeframe
	if tf, err = cli.GetWellTimeframe(indexer, well); err != nil {
		return
	}

	var shards []string
	if shards, err = cli.GetWellShardsInTimeframe(indexer, well, tf); err != nil {
		return
	}
	lgr.Infof("Shards: %v", shards)
	return
}

func getShard(cli *client.Client, indexer, well string) (shard string, err error) {
	if shard = *fShard; shard == `` {
		var tf util.Timeframe
		if tf, err = cli.GetWellTimeframe(indexer, well); err != nil {
			return
		}
		var shards []string
		if shards, err = cli.GetWellShardsInTimeframe(indexer, well, tf); err != nil {
			return
		}
		prompt := promptui.Select{
			Label: `Select Shard`,
			Items: shards,
		}
		if _, shard, err = prompt.Run(); err != nil {
			return
		}
	}
	return
}

func PullShard(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var indexer, well, shard string
	if indexer, err = getIndexer(cli); err != nil {
		return
	}
	if well, err = getWell(cli, indexer); err != nil {
		return
	}
	if shard, err = getShard(cli, indexer, well); err != nil {
		return
	}

	var storePath string
	var shardPath string
	if len(args) > 0 {
		storePath = args[0]
		if err = isDir(storePath); err != nil {
			return
		}
	} else {
		pmpt := promptui.Prompt{
			Label:    "Storage Path",
			Validate: isDir,
		}
		if storePath, err = pmpt.Run(); err != nil {
			return
		}
	}
	shardPath = filepath.Join(storePath, shard)
	if err = os.MkdirAll(shardPath, 0770); err != nil {
		return
	}
	var guid uuid.UUID
	if guid, err = uuid.Parse(indexer); err != nil {
		return
	}

	cancel := make(chan bool, 1)
	defer close(cancel)

	sid := client.ShardID{
		Indexer: guid,
		Well:    well,
		Shard:   shard,
	}

	err = cli.PullShard(sid, shardPath, cancel)
	return
}

func PushShard(cli *client.Client, tm tags.TagManager, lgr *log.Logger) (err error) {
	var tps []tags.TagPair
	var shardPath string
	var wellName string
	var shardId string
	if tps, err = tm.TagSet(); err != nil {
		return
	}
	tgs := []string{`test`, `test2`}

	cancel := make(chan bool, 1)
	defer close(cancel)
	if shardPath, wellName, shardId, err = getShardPath(); err != nil {
		return
	}
	lgr.Info("pushing shard")
	sid := client.ShardID{
		Indexer: guid,
		Well:    wellName,
		Shard:   shardId,
	}
	err = cli.PushShard(sid, shardPath, tps, tgs, cancel)
	return
}

func getShardPath() (shardPath, wellName, shardId string, err error) {
	validate := func(s string) error {
		_, _, err := getPathParts(s)
		return err
	}
	if len(args) > 0 {
		shardPath = args[0]
	} else {
		prompt := promptui.Prompt{
			Label:    "Shard Path",
			Validate: validate,
		}
		if shardPath, err = prompt.Run(); err != nil {
			return
		}
	}
	wellName, shardId, err = getPathParts(shardPath)
	return
}

func getPathParts(v string) (wellName, shardId string, err error) {
	v = filepath.Clean(v)
	if err = isDir(v); err == nil {
		//grab the base component
		shardId = filepath.Base(v)
		if wellName = filepath.Base(filepath.Dir(v)); wellName == `` || wellName == `.` {
			err = errors.New("Invalid well in path")
		} else {
			_, err = strconv.ParseUint(shardId, 16, 22) //up to 22 bits for a shardID
		}
	}
	return
}

func isDir(v string) (err error) {
	//check that path is a directory
	var fi os.FileInfo
	if fi, err = os.Stat(v); err == nil {
		if !fi.IsDir() {
			err = errors.New("Path is not a directory")
		}
	}
	return
}
