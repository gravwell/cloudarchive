/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package webserver

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"
)

func (w *Webserver) customerListIndexers(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	idx, err := w.shardHandler.ListIndexes(custID)
	if err != nil {
		serverFail(res, err)
		return
	}
	sendObject(res, idx)
}

func (w *Webserver) indexerListWells(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	// Get the indexer UUID
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	wells, err := w.shardHandler.ListIndexerWells(custID, indexerUUID)
	if err != nil {
		serverFail(res, err)
		return
	}
	sendObject(res, wells)
}

func (w *Webserver) indexerGetTags(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	// Get the indexer UUID
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	tgs, err := w.shardHandler.GetTags(custID, indexerUUID)
	if err != nil {
		serverFail(res, err)
		return
	}
	sendObject(res, tgs)
}

func (w *Webserver) indexerSyncTags(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	// Get the indexer UUID
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	// read out the tags the indexer sent us
	var idxTags []tags.TagPair
	if err := getObject(req, &idxTags); err != nil {
		serverFail(res, err)
		return
	}

	tgs, err := w.shardHandler.SyncTags(custID, indexerUUID, idxTags)
	if err != nil {
		serverFail(res, err)
		return
	}
	sendObject(res, tgs)
}

func (w *Webserver) getWellTimeframe(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	// Get the indexer UUID
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	// Get the well name
	well, err := getMuxString(req, "well")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	t, err := w.shardHandler.GetWellTimeframe(custID, indexerUUID, well)
	if err != nil {
		serverFail(res, err)
		return
	}
	sendObject(res, t)
}

func (w *Webserver) getWellShardsInTimeframe(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	// Get the customer ID
	custID, err := getMuxUint64(req, "custid")
	if err != nil {
		serverInvalid(res, err)
		return
	}
	// Get the indexer UUID
	indexerUUID, err := getMuxUUID(req, "uuid")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	// Get the well name
	well, err := getMuxString(req, "well")
	if err != nil {
		serverInvalid(res, err)
		return
	}

	if custID != cust.CustomerNumber {
		// Wrong customer!
		serverInvalid(res, errors.New("Wrong customer number"))
		return
	}

	// Now get the arguments
	var tf util.Timeframe
	if err := getObject(req, &tf); err != nil {
		serverFail(res, err)
		return
	}

	if tf.End.Before(tf.Start) {
		serverInvalid(res, fmt.Errorf("Invalid start time %v after end time %v", tf.Start, tf.End))
		return
	} else if tf.Start.IsZero() || tf.End.IsZero() {
		serverInvalid(res, fmt.Errorf("Start/end times must not be zero"))
		return
	}

	// Walk the list of shards we have for this well, grabbing
	// those which fall within the time range.
	shards, err := w.shardHandler.GetShardsInTimeframe(custID, indexerUUID, well, tf)
	if err != nil {
		serverFail(res, err)
		return
	}

	// Return the list
	sendObject(res, shards)
}
