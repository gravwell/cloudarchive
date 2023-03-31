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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gravwell/cloudarchive/pkg/shardpacker"
	"github.com/gravwell/gravwell/v3/ingest/entry"
)

const (
	ShardSet          int64  = 0x1ffff //this 1.517
	shardMaskBitCount uint64 = 17      //number of bits to remove when generating an a name
	shardQuant        int64  = ShardSet + 1
)

var (
	shardMask int64 = ^ShardSet
)

type ShardID int64

func ShardNameToDateRange(nm string) (s, e time.Time, err error) {
	// First trim the name
	nm = trimVersion(nm)
	var v int64
	if v, err = strconv.ParseInt(nm, 16, 64); err != nil {
		return
	}
	st := entry.Timestamp{
		Sec: v << shardMaskBitCount,
	}
	et := entry.Timestamp{
		Sec: int64(NextShardId(ShardID(v << shardMaskBitCount))),
	}
	s = st.StandardTime()
	e = et.StandardTime()
	return
}

// this should generally be inlined everywhere
func GetShardId(t time.Time) ShardID {
	return ShardID(entry.FromStandard(t).Sec & shardMask)
}

func NextShardId(curr ShardID) ShardID {
	return ShardID((int64(curr) & shardMask) + shardQuant)
}

func AddShardFilesToPacker(spath, id string, pkr *shardpacker.Packer) (err error) {
	id = trimVersion(id)
	//grab the verify file
	if err = addFile(spath, id, shardpacker.Verify, pkr, true); err != nil {
		return
	}
	//grab the index file
	if err = addFile(spath, id, shardpacker.Index, pkr, false); err != nil {
		return
	}
	//grab the store file
	if err = addFile(spath, id, shardpacker.Store, pkr, false); err != nil {
		return
	}

	//check which type of accelerator is in use (if there is one)
	var fi os.FileInfo
	if fi, err = os.Stat(filepath.Join(spath, shardpacker.AccelFile.Filename(id))); err != nil {
		//if it doesn't exist thats fine
		if !os.IsNotExist(err) {
			return
		} else {
			err = nil
		}
	} else {
		if fi.Mode().IsRegular() {
			//just push the file
			if err = addFile(spath, id, shardpacker.AccelFile, pkr, false); err != nil {
				return
			}
		} else {
			//push the components
			if err = addFile(spath, id, shardpacker.IndexAccelKeyFile, pkr, false); err != nil {
				pkr.CloseWithError(err)
			}
			if err = addFile(spath, id, shardpacker.IndexAccelDataFile, pkr, false); err != nil {
				return
			}
		}
	}
	return
}

func addFile(spath, id string, tp shardpacker.Ftype, pkr *shardpacker.Packer, optional bool) error {
	pth := filepath.Join(spath, tp.Filepath(id))
	if fin, sz, err := getHandleAndSize(pth); err != nil {
		if os.IsNotExist(err) && optional {
			return nil
		}
		return err
	} else if err = pkr.AddFile(tp, sz, fin); err != nil {
		fin.Close()
		return err
	} else if err = fin.Close(); err != nil {
		return err
	}
	return nil
}

func getHandleAndSize(p string) (fio *os.File, sz int64, err error) {
	var fi os.FileInfo
	if fio, err = os.Open(p); err != nil {
		return
	} else if fi, err = fio.Stat(); err != nil {
		fio.Close()
		return
	} else if !fi.Mode().IsRegular() {
		fio.Close()
		err = errors.New("not a regular file")
		return
	}
	sz = fi.Size()
	return
}

func trimVersion(nm string) string {
	return strings.TrimSuffix(nm, filepath.Ext(nm))
}
