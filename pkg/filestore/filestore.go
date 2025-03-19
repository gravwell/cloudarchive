/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package filestore

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gravwell/cloudarchive/pkg/shardpacker"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

var (
	ErrMissingBaseDir = errors.New("Empty base directory for file store")
)

type filestore struct {
	util.UploadTracker
	basedir string
}

func NewFilestoreHandler(bdir string) (*filestore, error) {
	if bdir == `` {
		return nil, ErrMissingBaseDir
	} else if err := writableDir(bdir); err != nil {
		return nil, err
	}
	return &filestore{
		basedir:       bdir,
		UploadTracker: util.NewUploadTracker(),
	}, nil
}

// just check that the filestore CAN
func (f *filestore) Preflight() (err error) {
	err = writableDir(f.basedir)
	return
}

func (f *filestore) ListIndexes(cid uint64) ([]string, error) {
	var idx []string
	custDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10))
	files, err := ioutil.ReadDir(custDir)
	if err != nil {
		return idx, err
	}
	for _, info := range files {
		if !info.IsDir() {
			continue
		}
		name := info.Name()
		if _, err := uuid.Parse(name); err == nil {
			idx = append(idx, name)
		}
	}
	return idx, err
}

func (f *filestore) ListIndexerWells(cid uint64, guid uuid.UUID) ([]string, error) {
	var wells []string
	idxDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), guid.String())
	files, err := ioutil.ReadDir(idxDir)
	if err != nil {
		return wells, err
	}
	for _, info := range files {
		if !info.IsDir() {
			continue
		}
		wells = append(wells, info.Name())
	}
	return wells, err
}

func (f *filestore) GetWellTimeframe(cid uint64, guid uuid.UUID, well string) (t util.Timeframe, err error) {
	wellDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), guid.String(), well)
	// we will play it safe and walk every file
	var files []os.FileInfo
	files, err = ioutil.ReadDir(wellDir)
	if err != nil {
		return
	}
	for _, info := range files {
		s, e, err := util.ShardNameToDateRange(info.Name())
		if err != nil {
			continue
		}
		if t.Start.IsZero() || s.Before(t.Start) {
			t.Start = s
		}
		if t.End.IsZero() || e.After(t.End) {
			t.End = e
		}
	}
	return
}

func (f *filestore) GetShardsInTimeframe(cid uint64, guid uuid.UUID, well string, tf util.Timeframe) (shards []string, err error) {
	wellDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), guid.String(), well)
	// we will play it safe and walk every file
	var files []os.FileInfo
	files, err = ioutil.ReadDir(wellDir)
	if err != nil {
		return
	}
	for _, info := range files {
		s, e, err := util.ShardNameToDateRange(info.Name())
		if err != nil {
			continue
		}
		// There are several ways for this to end up on the list:
		switch {
		// the start of the span falls within the shard
		case s.Before(tf.Start) && e.After(tf.Start):
			fallthrough
		// the end of the span falls within the shard
		case s.Before(tf.End) && e.After(tf.End):
			fallthrough
		// the span's start/end lands directly on the shard's start/end
		case s.Equal(tf.End) || s.Equal(tf.Start) || e.Equal(tf.End) || e.Equal(tf.Start):
			fallthrough
		// the span entirely contains the shard
		case tf.Start.Before(s) && tf.End.After(e):
			shards = append(shards, info.Name())
		}
	}
	return

}

func (f *filestore) UnpackShard(cid uint64, idxUUID uuid.UUID, well, shard string, rdr io.Reader) (err error) {
	var up *shardpacker.Unpacker
	uid := util.UploadID{
		CID:     cid,
		IdxUUID: idxUUID,
		Well:    well,
		Shard:   shard,
	}

	//create directory structure if it does not exist

	if err = f.EnterUpload(uid); err != nil {
		return
	}

	//generate the complete path to the customer/indexer upload location and make it
	//this will create all nessasary directories
	indexerDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), idxUUID.String())

	//do the same for the shard upload location
	shardDir := filepath.Join(indexerDir, well, shard)
	base := shardDir
	// Check if this shard already exists. If so, we'll keep adding .N suffixes until it works
	// We'll try up to some arbitrary big number... but we won't create shards infinitely forever,
	// in case an indexer is somehow misconfigured.
	for i := 1; i < 10000; i++ {
		if _, err := os.Stat(shardDir); errors.Is(err, os.ErrNotExist) {
			break
		}
		shardDir = fmt.Sprintf("%s.%d", base, i)
	}
	if err = os.MkdirAll(shardDir, 0770); err != nil {
		f.ExitUpload(uid)
		return
	}

	h := handler{
		cid:  cid,
		sdir: shardDir,
		bdir: indexerDir,
		guid: idxUUID,
	}
	//generate a new shard unpacker
	if up, err = shardpacker.NewUnpacker(shard, rdr); err != nil {
		os.RemoveAll(shardDir)
		f.ExitUpload(uid)
		return
	}
	//perform the actual unpack
	if err = up.Unpack(h); err != nil {
		os.RemoveAll(shardDir)
		f.ExitUpload(uid)
		return
	}

	//release the shard
	err = f.ExitUpload(uid)
	return
}

func (f *filestore) PackShard(cid uint64, idxUUID uuid.UUID, well, shard string, wtr io.Writer) (err error) {
	uid := util.UploadID{
		CID:     cid,
		IdxUUID: idxUUID,
		Well:    well,
		Shard:   shard,
	}
	p := shardpacker.NewPacker(shard)

	if err = f.EnterUpload(uid); err != nil {
		return
	}

	//generate the complete path to the customer/indexer upload location and make it
	//this will create all nessasary directories
	indexerDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), idxUUID.String())

	//do the same for the shard upload location
	shardDir := filepath.Join(indexerDir, well, shard)
	if err = readableDir(shardDir); err != nil {
		f.ExitUpload(uid)
		return
	}

	//fire up the routine that will relay from the packer to the writer
	copyErrChan := make(chan error, 1)
	defer close(copyErrChan)
	go func(ch chan error) {
		_, err := io.Copy(wtr, p)
		ch <- err
	}(copyErrChan)

	addFilesErrChan := make(chan error, 1)
	defer close(addFilesErrChan)
	go func(ch chan error) {
		err := util.AddShardFilesToPacker(shardDir, shard, p)
		if err != nil {
			p.CloseWithError(err)
		} else if err = p.Flush(); err != nil {
			p.CloseWithError(err)
		} else if err = p.Close(); err != nil {
			p.CloseWithError(err)
		}
		ch <- err
	}(addFilesErrChan)

	select {
	case err = <-copyErrChan:
		if err != nil {
			//somehow the copy chan exited first, close down teh file adder and wait
			p.CloseWithError(err)
			<-addFilesErrChan
		} else {
			//clean close on copy, wait for add files... This SHOULD never happen
			err = <-addFilesErrChan //this SHOULD happen first
		}
	case err = <-addFilesErrChan:
		if err != nil {
			//bomb it out and wait for the copy routine to exit
			p.CloseWithError(err) //just in case
			<-copyErrChan
		} else {
			//clean close, check the error coming off of the copy routine
			err = <-copyErrChan
		}
	}

	//release the shard, setting error appropriately
	if err == nil {
		err = f.ExitUpload(uid)
	} else {
		f.ExitUpload(uid)
	}

	return
}

func (f *filestore) GetTags(cid uint64, guid uuid.UUID) (tgs []tags.TagPair, err error) {
	var tm tags.TagManager
	indexerDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), guid.String())
	if tm, err = tags.GetTagMan(cid, guid, indexerDir); err != nil {
		return
	}
	tgs, err = tm.TagSet()
	if err == nil {
		err = tags.ReleaseTagMan(cid, guid) //set the error on release
	} else {
		tags.ReleaseTagMan(cid, guid) //we are in an error state, so just release
	}
	return
}

func (f *filestore) SyncTags(cid uint64, guid uuid.UUID, idxTags []tags.TagPair) (tgs []tags.TagPair, err error) {
	var tm tags.TagManager
	indexerDir := filepath.Join(f.basedir, strconv.FormatUint(cid, 10), guid.String())
	// This is likely to happen before the shard is synced, so make sure the directory exists
	if err = writableDir(indexerDir); err != nil {
		if err = os.MkdirAll(indexerDir, 0770); err != nil {
			return
		}
	}
	if tm, err = tags.GetTagMan(cid, guid, indexerDir); err != nil {
		return
	}
	// Now merge
	_, err = tm.Merge(idxTags)
	if err != nil {
		tags.ReleaseTagMan(cid, guid)
		return
	}
	// Fetch the updated tagset to return
	tgs, err = tm.TagSet()
	if err == nil {
		err = tags.ReleaseTagMan(cid, guid) //set the error on release
	} else {
		tags.ReleaseTagMan(cid, guid) //we are in an error state, so just release
	}
	return
}

type handler struct {
	cid  uint64    //customer number
	sdir string    //shard directory
	bdir string    //base directory
	guid uuid.UUID //indexer GUID
}

func (h handler) HandleFile(pth string, rdr io.Reader) error {
	//clean the path to ensure there are no relative path items
	dir, file := clean(pth)
	if dir != `` {
		err := os.Mkdir(filepath.Join(h.sdir, dir), 0770)
		if err != nil && !os.IsExist(err) {
			return err
		}
	}
	fout, err := os.Create(filepath.Join(h.sdir, dir, file))
	if err != nil {
		return err
	}
	if _, err := io.Copy(fout, rdr); err != nil {
		fout.Close()
		return err
	}
	return fout.Close()
}

func (h handler) HandleTagUpdate(tgs []tags.TagPair) error {
	//grab a tag manager handle
	tm, err := tags.GetTagMan(h.cid, h.guid, h.bdir)
	if err != nil {
		return err
	}
	if _, err = tm.Merge(tgs); err != nil {
		tags.ReleaseTagMan(h.cid, h.guid)
		return err
	}
	//release the tag manager handle
	return tags.ReleaseTagMan(h.cid, h.guid)
}

// clean removes any relative path elements and returns a potential single directory and file
func clean(p string) (d, f string) {
	p = filepath.Clean(p)
	//remove any starting . and do it again
	d, f = filepath.Split(filepath.Clean(strings.TrimLeft(p, "./")))
	if d = filepath.Base(d); d == `.` {
		d = ``
	}
	return
}

// writableDir ensures that the provided location exists, is a dir, and is R/W
func writableDir(pth string) error {
	if err := readableDir(pth); err != nil {
		return err
	} else if err = unix.Access(pth, unix.W_OK); err != nil {
		return err
	}
	return nil
}

// readableDir ensures that the provided location exists, is a dir, and is R/W
func readableDir(pth string) error {
	if fi, err := os.Stat(pth); err != nil {
		return err
	} else if !fi.Mode().IsDir() {
		return errors.New("not a directory")
	} else if err = unix.Access(pth, unix.R_OK); err != nil {
		return err
	}
	return nil
}
