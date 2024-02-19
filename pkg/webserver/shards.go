/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package webserver

import (
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v4/ingest/entry"
	"github.com/gravwell/gravwell/v4/ingest/log"
)

var (
	transferTickTimeout = 30 * time.Second
)

type ShardHandler interface {
	UnpackShard(cid uint64, guid uuid.UUID, well, shard string, rdr io.Reader) error
	PackShard(cid uint64, guid uuid.UUID, well, shard string, wtr io.Writer) error
	ListIndexes(cid uint64) ([]string, error)
	ListIndexerWells(cid uint64, guid uuid.UUID) ([]string, error)
	GetWellTimeframe(cid uint64, guid uuid.UUID, well string) (util.Timeframe, error)
	GetShardsInTimeframe(cid uint64, guid uuid.UUID, well string, tf util.Timeframe) (shards []string, err error)
	GetTags(cid uint64, guid uuid.UUID) ([]tags.TagPair, error)
	SyncTags(cid uint64, guid uuid.UUID, idxTags []tags.TagPair) (tgs []tags.TagPair, err error)
}

func (w *Webserver) shardPushHandler(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	defer req.Body.Close()
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	well, err := getMuxString(req, "well")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	shard, err := getMuxString(req, "shardid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}
	rdr, err := newRateTimeoutReader(req.Body, transferTickTimeout, res)
	if err != nil {
		serverFail(res, err)
		return
	}
	defer rdr.Close()

	w.lgr.Info("Shard push", log.KV("cid", custID), log.KV("indexeruuid", indexerUUID), log.KV("well", well), log.KV("shard", shard))
	if err = w.shardHandler.UnpackShard(custID, indexerUUID, well, shard, rdr); err != nil {
		w.lgr.Error("Failed to unpack shard", log.KV("cid", custID), log.KV("indexeruuid", indexerUUID), log.KV("well", well), log.KV("shard", shard), log.KVErr(err))
		serverFail(res, err)
	} else {
		res.WriteHeader(http.StatusOK)
	}
}

func (w *Webserver) shardPullHandler(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	defer req.Body.Close()
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	well, err := getMuxString(req, "well")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	shard, err := getMuxString(req, "shardid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}
	wtr, err := newRateTimeoutWriter(res, transferTickTimeout)
	if err != nil {
		serverFail(res, err)
		return
	}
	defer wtr.Close()

	w.lgr.Info("Shard pull", log.KV("cid", custID), log.KV("indexeruuid", indexerUUID), log.KV("well", well), log.KV("shard", shard))
	if err = w.shardHandler.PackShard(custID, indexerUUID, well, shard, wtr); err != nil {
		w.lgr.Error("Failed to pack shard", log.KV("cid", custID), log.KV("indexeruuid", indexerUUID), log.KV("well", well), log.KV("shard", shard), log.KVErr(err))
		serverFail(res, err)
	} else {
		res.WriteHeader(http.StatusOK)
	}
}

// mock handler for use in testing
type HashHandler struct {
	Hash []byte
}

func (hh *HashHandler) ListIndexes(cid uint64) (r []string, err error) {
	return
}

func (hh *HashHandler) ListIndexerWells(cid uint64, guid uuid.UUID) (r []string, err error) {
	return
}

func (hh *HashHandler) GetWellTimeframe(cid uint64, guid uuid.UUID, well string) (t util.Timeframe, err error) {
	t.Start = time.Now().Add(-5 * time.Minute)
	t.End = time.Now()
	return
}

func (hh *HashHandler) GetShardsInTimeframe(cid uint64, guid uuid.UUID, well string, tf util.Timeframe) (shards []string, err error) {
	return
}

func (hh *HashHandler) UnpackShard(custid uint64, indexerUUID uuid.UUID, well string, shardID string, reader io.Reader) error {
	hasher := sha256.New()
	io.Copy(hasher, reader)
	hh.Hash = hasher.Sum(nil)
	return nil
}

func (hh *HashHandler) PackShard(custid uint64, indexerUUID uuid.UUID, well string, shardID string, wtr io.Writer) error {
	return errors.New("HashHandler is a write only, no retrieval")
}

func (hh *HashHandler) GetTags(custid uint64, indexerUUID uuid.UUID) (tgs []tags.TagPair, err error) {
	tgs = []tags.TagPair{
		tags.TagPair{Name: entry.DefaultTagName, Value: 0},
		tags.TagPair{Name: entry.GravwellTagName, Value: 0xffff},
	}
	return
}

func (hh *HashHandler) SyncTags(custid uint64, indexerUUID uuid.UUID) (tgs []tags.TagPair, err error) {
	// ignore the update, just send back the default.
	tgs = []tags.TagPair{
		tags.TagPair{Name: entry.DefaultTagName, Value: 0},
		tags.TagPair{Name: entry.GravwellTagName, Value: 0xffff},
	}
	return
}
