/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package client

import (
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

const (
	//login field names
	USER_FIELD string = "User"
	PASS_FIELD string = "Pass"

	//path to login url
	LOGIN_URL      = `/api/login`
	TEST_URL       = `/api/test`
	TEST_AUTH_URL  = `/api/testauth`
	PUSH_SHARD_URL = `/api/shard/%v/%v/%v/%v`
)

type ClientSource interface {
	io.Reader
	Cancel()
}

type readTicker struct {
	rdr      io.Reader
	maxChunk int
	tckchan  chan bool
}

type ShardID struct {
	Indexer uuid.UUID //indexer GUID
	Well    string    //well name
	Shard   string    //shard ID
}

func (sid ShardID) PushShardUrl(custID uint64) string {
	return fmt.Sprintf(PUSH_SHARD_URL, custID, sid.Indexer, sid.Well, sid.Shard)
}

func newReadTicker(rdr io.Reader, maxChunk int) (*readTicker, error) {
	if maxChunk <= 0 {
		return nil, errors.New("Invalid chunk size")
	} else if rdr == nil {
		return nil, errors.New("invalid reader")
	}
	return &readTicker{
		rdr:      rdr,
		maxChunk: maxChunk,
		tckchan:  make(chan bool, 1),
	}, nil
}

func (rt *readTicker) ticker() <-chan bool {
	return rt.tckchan
}

func (rt *readTicker) Read(b []byte) (n int, err error) {
	//reset the buffer size
	if len(b) > rt.maxChunk {
		b = b[0:rt.maxChunk]
	}
	if n, err = rt.rdr.Read(b); err == nil {
		//attempt to tick, if we can't write, thats fine
		select {
		case rt.tckchan <- true:
		default: //do nothing
		}
	}
	return
}
