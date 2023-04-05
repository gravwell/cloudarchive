/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package util

import (
	"errors"
	"sync"

	"github.com/google/uuid"
)

var (
	empty                  es
	ErrUploadInProgress    = errors.New("Shard upload already in progress")
	ErrUploadNotInProgress = errors.New("Shard upload not in progress")
)

// empty struct for efficient map storage
type es struct{}

type UploadTracker struct {
	sync.Mutex
	active map[UploadID]es
}

func NewUploadTracker() UploadTracker {
	return UploadTracker{
		active: make(map[UploadID]es, 16),
	}
}

type UploadID struct {
	CID     uint64    //customer ID
	IdxUUID uuid.UUID //indexer UUID
	Well    string    //well name
	Shard   string    //shard ID
}

// EnterUpload attempts to claim an upload ID, if an upload with the existing ID
// exists an error is returnd
func (t *UploadTracker) EnterUpload(uid UploadID) (err error) {
	t.Lock()
	if _, ok := t.active[uid]; ok {
		err = ErrUploadInProgress
	} else {
		t.active[uid] = empty
	}
	t.Unlock()
	return
}

// ExitUpload releases an existing upload lock for a given shard
// if the shard wasn't locked, and error is returned
func (t *UploadTracker) ExitUpload(uid UploadID) (err error) {
	t.Lock()
	if _, ok := t.active[uid]; ok {
		delete(t.active, uid)
	} else {
		err = ErrUploadNotInProgress
	}
	t.Unlock()
	return
}
