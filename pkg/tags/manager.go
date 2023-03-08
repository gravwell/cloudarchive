/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package tags

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
)

type keystr struct {
	id   uint64
	guid uuid.UUID
}

type vset struct {
	tm      *TagMan
	handles int
}

var (
	mtx     *sync.Mutex
	tagSets map[keystr]vset

	ErrManagerClosed   = errors.New("Manager is closed")
	ErrOpenHandles     = errors.New("tag manager has open handles is closed")
	ErrNoActiveHandles = errors.New("tag manager is not active")
)

func init() {
	mtx = &sync.Mutex{}
	tagSets = make(map[keystr]vset, 8)
}

func CloseTagSets() (err error) {
	mtx.Lock()
	if tagSets != nil {
		for k, v := range tagSets {
			if v.handles > 0 {
				err = ErrOpenHandles
				break
			} else if err = v.tm.Close(); err != nil {
				break
			}
			delete(tagSets, k)
		}
	}
	tagSets = nil
	mtx.Unlock()
	return
}

func GetTagMan(id uint64, guid uuid.UUID, basedir string) (tm *TagMan, err error) {
	var ok bool
	var v vset
	k := keystr{
		id:   id,
		guid: guid,
	}
	tpath := filepath.Join(basedir, TAG_MANAGER_FILENAME)
	mtx.Lock()
	if tagSets == nil {
		err = ErrManagerClosed
	} else if v, ok = tagSets[k]; !ok || v.handles == 0 {
		if v.tm, err = New(tpath); err == nil {
			v.handles++
			tm = v.tm
			tagSets[k] = v
		}
	} else {
		v.handles++
		tm = v.tm
		tagSets[k] = v
	}
	mtx.Unlock()
	return
}

func ReleaseTagMan(id uint64, guid uuid.UUID) (err error) {
	var ok bool
	var v vset
	k := keystr{
		id:   id,
		guid: guid,
	}
	mtx.Lock()
	if tagSets == nil {
		err = ErrManagerClosed
	} else if v, ok = tagSets[k]; !ok || v.handles == 0 {
		err = ErrNoActiveHandles
	} else {
		//got a handle and its active
		v.handles--
		if v.handles == 0 {
			//this is the last handle, close and delete from set
			if err = v.tm.Close(); err == nil {
				delete(tagSets, k)
			}
		} else {
			tagSets[k] = v //just decrement and assign back in
		}
	}
	mtx.Unlock()
	return
}
