/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

// Package ftpstore implements the FTP storage plugin for Gravwell CloudArchive
package ftpstore

import (
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gravwell/cloudarchive/pkg/shardpacker"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/jlaffaye/ftp"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

var (
	ErrMissingBaseDir = errors.New("Empty base directory for file store")

	errNotImplemented = `502 Command not implemented.`

	ftpSync sync.Mutex
)

type ftpstore struct {
	cfg FtpStoreConfig
	util.UploadTracker
}

type FtpStoreConfig struct {
	FtpServer  string // addr:port
	LocalStore string // path where we can keep some files locally
	BaseDir    string // base directory *on the server*
	Username   string
	Password   string
	Lgr        *log.Logger
}

func NewFtpStoreHandler(cfg FtpStoreConfig) (*ftpstore, error) {
	if cfg.Lgr == nil {
		cfg.Lgr = log.New(os.Stderr)
	}
	return &ftpstore{
		cfg:           cfg,
		UploadTracker: util.NewUploadTracker(),
	}, nil
}

// make sure we can login, list, and put a test file to the base directory
func (f *ftpstore) Preflight() (err error) {
	var conn *ftp.ServerConn
	pfstring := fmt.Sprintf("preflight test %v", time.Now())
	if conn, err = f.getFtpClient(); err != nil {
		return
	} else if _, err = conn.List(f.cfg.BaseDir); err != nil {
		conn.Quit()
		return
	} else if err = conn.Stor(".preflight_test", strings.NewReader(pfstring)); err != nil {
		conn.Quit()
		return
	}
	err = conn.Quit()
	return
}

func (f *ftpstore) Close() (err error) {
	return
}

func (f *ftpstore) getFtpClient() (*ftp.ServerConn, error) {
	do := ftp.DialWithTimeout(10 * time.Second)
	c, err := ftp.Dial(f.cfg.FtpServer, do)
	if err != nil {
		f.cfg.Lgr.Error("Failed to dial server", log.KV("address", f.cfg.FtpServer), log.KVErr(err))
		return nil, err
	}
	if err = c.Login(f.cfg.Username, f.cfg.Password); err != nil {
		f.cfg.Lgr.Error("Failed to log in", log.KV("address", f.cfg.FtpServer), log.KVErr(err))
	}
	return c, err
}

func (f *ftpstore) ListIndexes(cid uint64) ([]string, error) {
	var indexes []string
	var ents []*ftp.Entry
	var err error
	c, err := f.getFtpClient()
	if err != nil {
		return indexes, err
	}
	custDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10))
	if ents, err = c.List(custDir); err != nil {
		return indexes, err
	}
	for _, info := range ents {
		if info.Type != ftp.EntryTypeFolder {
			continue
		}
		name := info.Name
		if _, err := uuid.Parse(name); err == nil {
			indexes = append(indexes, name)
		}
	}
	return indexes, err
}

func (f *ftpstore) ListIndexerWells(cid uint64, guid uuid.UUID) ([]string, error) {
	var wells []string
	var ents []*ftp.Entry
	var err error
	c, err := f.getFtpClient()
	if err != nil {
		return wells, err
	}
	idxDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), guid.String())
	if ents, err = c.List(idxDir); err != nil {
		f.cfg.Lgr.Error("Failed to list index directory",
			log.KV("directory", idxDir),
			log.KVErr(err))
		return wells, err
	}
	for _, info := range ents {
		if info.Type != ftp.EntryTypeFolder {
			continue
		}
		wells = append(wells, info.Name)
	}
	return wells, err
}

func (f *ftpstore) GetWellTimeframe(cid uint64, guid uuid.UUID, well string) (t util.Timeframe, err error) {
	var c *ftp.ServerConn
	c, err = f.getFtpClient()
	if err != nil {
		return
	}
	wellDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), guid.String(), well)
	// we will play it safe and walk every file
	var ents []*ftp.Entry
	ents, err = c.List(wellDir)
	if err != nil {
		f.cfg.Lgr.Error("Failed to list well directory",
			log.KV("directory", wellDir),
			log.KVErr(err))
		return
	}
	for _, info := range ents {
		s, e, err := util.ShardNameToDateRange(info.Name)
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

func (f *ftpstore) GetShardsInTimeframe(cid uint64, guid uuid.UUID, well string, tf util.Timeframe) (shards []string, err error) {
	var c *ftp.ServerConn
	c, err = f.getFtpClient()
	if err != nil {
		return
	}
	wellDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), guid.String(), well)
	// we will play it safe and walk every file
	var ents []*ftp.Entry
	ents, err = c.List(wellDir)
	if err != nil {
		f.cfg.Lgr.Error("Failed to list well directory",
			log.KV("directory", wellDir),
			log.KVErr(err))
		return
	}
	for _, info := range ents {
		s, e, err := util.ShardNameToDateRange(info.Name)
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
			shards = append(shards, info.Name)
		}
	}
	return
}

func (f *ftpstore) UnpackShard(cid uint64, idxUUID uuid.UUID, well, shard string, rdr io.Reader) (err error) {
	var up *shardpacker.Unpacker
	uid := util.UploadID{
		CID:     cid,
		IdxUUID: idxUUID,
		Well:    well,
		Shard:   shard,
	}

	if err = f.EnterUpload(uid); err != nil {
		f.cfg.Lgr.Error("Failed to enter upload", log.KVErr(err))
		return
	}

	c, err := f.getFtpClient()
	if err != nil {
		f.ExitUpload(uid)
		return err
	}

	//generate the complete path to the customer/indexer upload location and make it
	//this will create all nessasary directories
	indexerDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), idxUUID.String())

	//do the same for the shard upload location
	shardDir := filepath.Join(indexerDir, well, shard)
	base := shardDir
	// Check if this shard already exists. If so, we'll keep adding .N suffixes until it works
	// We'll try up to some arbitrary big number... but we won't create shards infinitely forever,
	// in case an indexer is somehow misconfigured.
	for i := 1; i < 10000; i++ {
		if !ftpDirExists(c, shardDir) {
			break
		}
		shardDir = fmt.Sprintf("%s.%d", base, i)
	}
	if err = ftpMkdirAll(c, shardDir); err != nil {
		f.ExitUpload(uid)
		f.cfg.Lgr.Error("Failed to make shard directory",
			log.KV("directory", shardDir),
			log.KVErr(err))
		return
	}

	h := handler{
		client:     c,
		localStore: f.cfg.LocalStore,
		cid:        cid,
		sdir:       shardDir,
		bdir:       indexerDir,
		guid:       idxUUID,
	}
	h.ensureTagsDat()
	//generate a new shard unpacker
	if up, err = shardpacker.NewUnpacker(shard, rdr); err != nil {
		c.RemoveDirRecur(shardDir)
		f.ExitUpload(uid)
		f.cfg.Lgr.Error("Failed to create new shard unpacker",
			log.KV("client-id", cid),
			log.KV("uuid", idxUUID),
			log.KV("shard", shardDir),
			log.KVErr(err))
		return
	}
	//perform the actual unpack
	if err = up.Unpack(h); err != nil {
		c.RemoveDirRecur(shardDir)
		f.ExitUpload(uid)
		f.cfg.Lgr.Error("Failed to unpack shard",
			log.KV("client-id", cid),
			log.KV("uuid", idxUUID),
			log.KV("shard", shardDir),
			log.KVErr(err))
		return
	}

	//release the shard
	err = f.ExitUpload(uid)
	return
}

func (f *ftpstore) PackShard(cid uint64, idxUUID uuid.UUID, well, shard string, wtr io.Writer) (err error) {
	c, err := f.getFtpClient()
	if err != nil {
		return err
	}
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

	// Figure out where we're pulling from
	indexerDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), idxUUID.String())
	shardDir := filepath.Join(indexerDir, well, shard)
	if !ftpDirExists(c, shardDir) {
		err = fmt.Errorf("Shard directory %v does not appear to exist on the server", shardDir)
		f.ExitUpload(uid)
		return
	}

	// Figure out where we're pulling to
	localShardDir := filepath.Join(f.cfg.LocalStore, strconv.FormatUint(cid, 10), idxUUID.String(), well, shard)
	if err = os.MkdirAll(localShardDir, 0770); err != nil {
		f.ExitUpload(uid)
		return
	}
	defer os.RemoveAll(localShardDir)

	// Copy everything over
	walker := c.Walk(shardDir)
	for walker.Next() {
		stat := walker.Stat()
		if stat.Type == ftp.EntryTypeFile {
			name := strings.TrimPrefix(walker.Path(), shardDir) // gives us e.g. "70cc2" or "70cc2.accel/data"
			if dir := filepath.Dir(name); dir != "" {
				if err = os.MkdirAll(filepath.Join(localShardDir, dir), 0770); err != nil {
					f.ExitUpload(uid)
					return
				}
			}
			fout, err := os.Create(filepath.Join(localShardDir, name))
			if err != nil {
				f.ExitUpload(uid)
				return err
			}
			defer fout.Close()
			resp, err := c.Retr(walker.Path())
			if err != nil {
				f.ExitUpload(uid)
				return err
			}
			if _, err := io.Copy(fout, resp); err != nil {
				f.ExitUpload(uid)
				resp.Close()
				return err
			}
			resp.Close()
		}
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
		err := util.AddShardFilesToPacker(localShardDir, shard, p)
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

func (f *ftpstore) GetTags(cid uint64, guid uuid.UUID) (tgs []tags.TagPair, err error) {
	var c *ftp.ServerConn
	c, err = f.getFtpClient()
	if err != nil {
		return
	}
	indexerDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), guid.String())
	h := handler{
		client:     c,
		localStore: f.cfg.LocalStore,
		cid:        cid,
		bdir:       indexerDir,
		guid:       guid,
	}
	h.ensureTagsDat()
	localBaseDir := filepath.Join(h.localStore, indexerDir)
	var tm tags.TagManager
	if tm, err = tags.GetTagMan(cid, guid, localBaseDir); err != nil {
		f.cfg.Lgr.Error("Failed enumerate tags", log.KVErr(err))
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

func (f *ftpstore) SyncTags(cid uint64, guid uuid.UUID, idxTags []tags.TagPair) (tgs []tags.TagPair, err error) {
	var c *ftp.ServerConn
	c, err = f.getFtpClient()
	if err != nil {
		return
	}
	indexerDir := filepath.Join(f.cfg.BaseDir, strconv.FormatUint(cid, 10), guid.String())
	h := handler{
		client:     c,
		localStore: f.cfg.LocalStore,
		cid:        cid,
		bdir:       indexerDir,
		guid:       guid,
	}
	h.ensureTagsDat()
	localBaseDir := filepath.Join(h.localStore, indexerDir)
	var tm tags.TagManager
	if tm, err = tags.GetTagMan(cid, guid, localBaseDir); err != nil {
		f.cfg.Lgr.Error("Failed enumerate tags", log.KVErr(err))
		return
	}
	// Now merge
	_, err = tm.Merge(idxTags)
	if err != nil {
		tags.ReleaseTagMan(cid, guid)
		f.cfg.Lgr.Error("Failed merge tags", log.KVErr(err))
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

func ftpDirExists(c *ftp.ServerConn, path string) bool {
	//first we try GetEntry, but if that is not implemented we will use List which is more expensive
	//check if its a 502 response of not-implemented which apparently is a thing
	if _, err := c.GetEntry(path); err == nil {
		return true
	} else if err.Error() == errNotImplemented {
		//ok, do the more expensive change directory command
		if cdir, err := c.CurrentDir(); err == nil {
			if err = c.ChangeDir(path); err == nil {
				if err = c.ChangeDir(cdir); err == nil {
					return true
				}
			}
		}
		return false
	}
	return false
}

func ftpMkdirAll(c *ftp.ServerConn, path string) error {
	// We grab the lock because this can be a little racy
	ftpSync.Lock()
	defer ftpSync.Unlock()
	dirs := strings.Split(path, string(os.PathSeparator))
	for i := range dirs {
		p := strings.Join(dirs[:i+1], string(os.PathSeparator))
		if !ftpDirExists(c, p) {
			if err := c.MakeDir(p); err != nil {
				return err
			}
		}
	}
	return nil
}

type handler struct {
	client     *ftp.ServerConn
	localStore string    // local storage directory, we keep tags.dat and such here
	cid        uint64    //customer number
	sdir       string    //shard directory
	bdir       string    //base directory
	guid       uuid.UUID //indexer GUID
}

func (h handler) HandleFile(pth string, rdr io.Reader) error {
	//clean the path to ensure there are no relative path items
	dir, file := clean(pth)
	if dir != `` {
		err := ftpMkdirAll(h.client, filepath.Join(h.sdir, dir))
		if err != nil {
			return err
		}
	}
	dest := filepath.Join(h.sdir, filepath.Join(dir, file))
	if err := h.client.Stor(dest, rdr); err != nil {
		return err
	}
	return nil
}

func (h handler) ensureTagsDat() error {
	// Grab the lock first, because we don't want to re-fetch tags.dat while
	// somebody else is in the middle of it
	ftpSync.Lock()
	defer ftpSync.Unlock()

	// Check if the appropriate tags.dat is on the disk
	tagpath := filepath.Join(h.localStore, tags.GetTagDatPath(h.bdir))
	if _, err := os.Stat(tagpath); err == nil {
		// exists, continue
		return nil
	} else if errors.Is(err, os.ErrNotExist) {
		// If not:
		// Create directory
		if err := os.MkdirAll(filepath.Dir(tagpath), 0770); err != nil {
			return err
		}
		// Open the file
		fout, err := os.Create(tagpath)
		if err != nil {
			return err
		}
		defer fout.Close()
		// Fetch from FTP and write to local file
		resp, err := h.client.Retr(tags.GetTagDatPath(h.bdir))
		if err != nil {
			// if it was a 551 error, that just means the file doesn't exist. That's fine.
			if e, ok := err.(*textproto.Error); ok {
				if e.Code == 551 {
					return nil // this will create an empty tags.dat file on the local store
				}
			}
			return err
		}
		defer resp.Close()
		if _, err := io.Copy(fout, resp); err != nil {
			return err
		}
	} else {
		return err // something else bad happened
	}

	return nil
}

func (h handler) pushTagsDat() error {
	// Grab the lock so we don't trounce anything
	ftpSync.Lock()
	defer ftpSync.Unlock()
	remotePath := tags.GetTagDatPath(h.bdir)
	localPath := filepath.Join(h.localStore, remotePath)
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return h.client.Stor(remotePath, f)
}

func (h handler) HandleTagUpdate(tgs []tags.TagPair) error {
	// Fetch tags.dat into the localstore dir if it doesn't exist
	if err := h.ensureTagsDat(); err != nil {
		return err
	}
	//grab a tag manager handle pointing at our tags.dat
	localBaseDir := filepath.Join(h.localStore, h.bdir)
	tm, err := tags.GetTagMan(h.cid, h.guid, localBaseDir)
	if err != nil {
		return err
	}
	if _, err = tm.Merge(tgs); err != nil {
		tags.ReleaseTagMan(h.cid, h.guid)
		return err
	}
	//release the tag manager handle
	if err := tags.ReleaseTagMan(h.cid, h.guid); err != nil {
		return err
	}
	// Push the result back up
	return h.pushTagsDat()
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
