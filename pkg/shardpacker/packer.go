/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package shardpacker

import (
	"archive/tar"
	"bytes"
	"compress/zlib"
	"context"
	"encoding/gob"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/dolmen-go/contextio"
	"github.com/gravwell/cloudarchive/pkg/tags"
)

const (
	Store              Ftype = 1
	Index              Ftype = 2
	Verify             Ftype = 3
	AccelFile          Ftype = 4 //for bloom filter accelerators
	IndexAccelKeyFile  Ftype = 5 //when sending fully indexed accelerators
	IndexAccelDataFile Ftype = 6
	TagsUpdate         Ftype = 7
	WellTags           Ftype = 8

	tagupdateFilename string = `tagsupdate`
	wellTagsFilename  string = `tags`
)

var (
	ErrClosed          = errors.New("closed")
	ErrInvalidFilePath = errors.New("invalid filepath")
	ErrInvalidFileType = errors.New("invalid file type")
)

type Ftype int

type Packer struct {
	io.ReadCloser
	sync.Mutex
	ftracker
	id   string
	ctx  context.Context
	cf   context.CancelFunc
	twtr *tar.Writer
	zwtr *zlib.Writer
	prdr *io.PipeReader
	pwtr *io.PipeWriter
}

type ftracker struct {
	storeHit      bool
	indexHit      bool
	verifyHit     bool
	accelHit      bool
	accelKeyHit   bool
	accelDataHit  bool
	wellTagsHit   bool
	tagsUpdateHit bool
}

func NewPacker(id string) (p *Packer) {
	p = &Packer{
		id: id,
	}
	p.ctx, p.cf = context.WithCancel(context.Background())
	p.prdr, p.pwtr = io.Pipe() //get a pipe wired up
	//get the compressing writer up wired to the pipe with a context wrapper
	p.zwtr = zlib.NewWriter(contextio.NewWriter(p.ctx, p.pwtr))
	p.twtr = tar.NewWriter(p.zwtr) //wire the tar writer to the compressed writer
	return
}

func (p *Packer) Flush() (err error) {
	p.Lock()
	if p.pwtr == nil || p.zwtr == nil || p.twtr == nil {
		err = errors.New("Already closed")
	} else {
		a := p.twtr
		b := p.zwtr
		if err = a.Flush(); err == nil {
			err = b.Flush()
		}
	}
	p.Unlock()
	return
}

func (p *Packer) Cancel() {
	if p.cf != nil {
		p.cf()
	}
	if p.pwtr != nil {
		p.pwtr.CloseWithError(errors.New("Cancelled"))
	}
}

func (p *Packer) CloseWithTimeout(to time.Duration) error {
	if to <= 0 {
		return p.Close()
	}
	tmr := time.AfterFunc(to, p.Cancel)
	defer tmr.Stop()
	return p.closePipeline()
}

// Close is used for the Nominal Close where no errors are passed to the reader
func (p *Packer) Close() (err error) {
	err = p.closePipeline()
	return
}

func (p *Packer) closePipeline() (err error) {
	p.Lock()
	defer p.Unlock()
	if p.pwtr == nil || p.zwtr == nil || p.twtr == nil {
		return errors.New("Already closed")
	}
	//flush and close in this order:
	// tar, zlib, pipe
	if err = p.twtr.Close(); err != nil {
		return
	} else if err = p.zwtr.Close(); err != nil {
		return
	}
	err = p.pwtr.Close()
	return
}

// Close with error is used to close but also send a read error to the pipe
func (p *Packer) CloseWithError(err error) error {
	p.Lock()
	defer p.Unlock()
	if p.pwtr == nil || p.zwtr == nil || p.twtr == nil {
		return ErrClosed
	}
	//close the pipe with an error
	if err = p.pwtr.CloseWithError(err); err != nil {
		return err
	}
	p.cf()
	//set all the writers to nil, basically a discard
	p.twtr.Close()
	p.zwtr.Close()
	return nil
}

// AddTags pushes a complete list of tag pairs, this is the complete mapping
// of tagname to tag id.  The webserver merges the tag set, potentially adding tags
func (p *Packer) AddTags(tps []tags.TagPair) (err error) {
	if tps == nil {
		tps = []tags.TagPair{} //we cannot hand in a nil
	}
	bb := bytes.NewBuffer(nil)
	if err = gob.NewEncoder(bb).Encode(tps); err != nil {
		return
	}

	return p.addByteStream(TagsUpdate, bb.Bytes())
}

// AddWellTags adds the list of tags that are assigned to the well
// every time we push a shard we push an updated set of tags that are assigned
// the default well will have an empty list
func (p *Packer) AddWellTags(tgs []string) (err error) {
	if tgs == nil {
		tgs = []string{} //we cannot hand in a nil
	}
	bb := bytes.NewBuffer(nil)
	for i := range tgs {
		io.WriteString(bb, tgs[i]+"\n")
	}
	return p.addByteStream(WellTags, bytes.TrimRight(bb.Bytes(), "\n"))
}

// addByteStream will take an object and Ftype and encode it into the tar file
func (p *Packer) addByteStream(tp Ftype, bts []byte) (err error) {
	pth := tp.Filename(p.id)
	if pth == `` {
		err = ErrInvalidFileType
		return
	}
	var twtr *tar.Writer
	sz := int64(len(bts))
	//lock and grab a local copy of the tar writer, if a close happens on the read
	//side while we are writing, we won't lose access to the tar writer
	p.Lock()
	if p.pwtr == nil || p.zwtr == nil || p.twtr == nil {
		err = ErrClosed
	} else {
		err = p.hitType(tp)
		twtr = p.twtr
	}
	p.Unlock()
	if err != nil {
		return
	}
	//create the tar header
	hdr := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     pth,
		Size:     sz,
		Mode:     0600,
		Format:   tar.FormatGNU,
	}
	if err = twtr.WriteHeader(&hdr); err != nil {
		return
	}
	err = writeAll(twtr, bts)
	return
}

func (p *Packer) AddFile(tp Ftype, sz int64, rdr io.Reader) (err error) {
	var twtr *tar.Writer
	pth := tp.Filename(p.id)
	if pth == `` {
		err = ErrInvalidFileType
		return
	}
	//lock and grab a local copy of the tar writer, if a close happens on the read
	//side while we are writing, we won't lose access to the tar writer
	p.Lock()
	if p.pwtr == nil || p.zwtr == nil || p.twtr == nil {
		err = ErrClosed
	} else {
		err = p.hitType(tp)
		twtr = p.twtr
	}
	p.Unlock()
	if err != nil {
		return
	}
	//create the tar header
	hdr := tar.Header{
		Typeflag: tar.TypeReg,
		Name:     pth,
		Size:     sz,
		Mode:     0600,
		Format:   tar.FormatGNU,
	}
	if err = twtr.WriteHeader(&hdr); err != nil {
		return
	}
	var n int64
	if n, err = io.CopyN(twtr, rdr, sz); err == nil && n != sz {
		err = errors.New("Failed file write")
	}
	return
}

// hitType marks the shard file type as added in the packer
// this ensures we can't add things twice or attempt to add two different accelerators
func (p *ftracker) hitType(tp Ftype) (err error) {
	switch tp {
	case Store:
		if p.storeHit {
			err = errors.New("Store file already added")
		}
		p.storeHit = true
	case Index:
		if p.indexHit {
			err = errors.New("Index file already added")
		}
		p.indexHit = true
	case Verify:
		if p.verifyHit {
			err = errors.New("Verify file already added")
		}
		p.verifyHit = true
	case AccelFile:
		if p.accelHit || p.accelKeyHit || p.accelDataHit {
			err = errors.New("Accelerator file already added")
		}
		p.accelHit = true
	case IndexAccelKeyFile:
		if p.accelHit || p.accelKeyHit {
			err = errors.New("Accelerator already added")
		}
		p.accelKeyHit = true
	case IndexAccelDataFile:
		if p.accelHit || p.accelDataHit {
			err = errors.New("Accelerator already added")
		}
		p.accelDataHit = true
	case TagsUpdate:
		if p.tagsUpdateHit {
			err = errors.New("Tags update already added")
		}
		p.tagsUpdateHit = true
	case WellTags:
		if p.wellTagsHit {
			err = errors.New("Well tags already added")
		}
		p.wellTagsHit = true
	default:
		err = errors.New("unknown type")
	}
	return
}

func (p *ftracker) allFilesHit(strict bool) (err error) {
	if !p.storeHit {
		err = errors.New("store file missing")
	} else if !p.tagsUpdateHit && strict {
		err = errors.New("tags update file missing")
	} else if !p.wellTagsHit && strict {
		err = errors.New("well tags file missing")
	} else if !p.indexHit && strict {
		err = errors.New("index file missing")
	} else if !p.verifyHit && strict {
		err = errors.New("verify file missing")
	} else if p.accelKeyHit || p.accelDataHit {
		if !p.accelKeyHit {
			err = errors.New("indexed accelerator key file missing")
		} else if !p.accelDataHit {
			err = errors.New("indexed accelerator data file missing")
		}
	}
	return
}

func (p *Packer) Read(b []byte) (n int, err error) {
	if p.prdr == nil {
		err = io.EOF
	} else {
		n, err = p.prdr.Read(b)
	}
	return
}

func (ft Ftype) Filename(id string) string {
	switch ft {
	case TagsUpdate:
		return tagupdateFilename
	case WellTags:
		return wellTagsFilename
	case Store:
		return id + ".store"
	case Index:
		return id + ".index"
	case Verify:
		return id + ".verify"
	case AccelFile:
		return id + ".accel"
	case IndexAccelKeyFile:
		return "keys"
	case IndexAccelDataFile:
		return "data"
	}
	return ``
}

func (ft Ftype) Filepath(id string) string {
	switch ft {
	case TagsUpdate:
		return tagupdateFilename
	case WellTags:
		return wellTagsFilename
	case Store:
		return id + ".store"
	case Index:
		return id + ".index"
	case Verify:
		return id + ".verify"
	case AccelFile:
		return id + ".accel"
	case IndexAccelKeyFile:
		return filepath.Join(AccelFile.Filename(id), "keys")
	case IndexAccelDataFile:
		return filepath.Join(AccelFile.Filename(id), "data")
	}
	return ``
}

func FilenameToType(name string) (ft Ftype, err error) {
	if name == `keys` {
		ft = IndexAccelKeyFile
		return
	} else if name == `data` {
		ft = IndexAccelDataFile
		return
	} else if name == tagupdateFilename {
		ft = TagsUpdate
		return
	} else if name == wellTagsFilename {
		ft = WellTags
		return
	}
	ext := filepath.Ext(name)
	switch ext {
	case `.store`:
		ft = Store
	case `.index`:
		ft = Index
	case `.verify`:
		ft = Verify
	case `.accel`:
		ft = AccelFile
	default:
		err = ErrInvalidFileType
	}
	return
}

func writeAll(wtr io.Writer, b []byte) (err error) {
	var n int
	var written int
	for written < len(b) {
		if n, err = wtr.Write(b[written:]); err != nil {
			break
		} else if n == 0 {
			err = errors.New("Failed write")
			break
		}
		written += n
	}
	return
}
