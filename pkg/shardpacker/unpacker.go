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
	"context"
	"encoding/gob"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dolmen-go/contextio"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/klauspost/compress/zlib"
)

var (
	ErrFailedWrite           = errors.New("Failed writing out complete file")
	ErrInvalidUnpackerParams = errors.New("Invalid unpacker parameters")
)

type UnpackHandler interface {
	HandleFile(string, io.Reader) error
	HandleTagUpdate([]tags.TagPair) error
}

type Unpacker struct {
	io.WriteCloser
	sync.Mutex
	ftracker
	ctx context.Context
	cf  context.CancelFunc
	rdr io.Reader
	id  string
}

func NewUnpacker(id string, rdr io.Reader) (up *Unpacker, err error) {
	id = trimVersion(id)
	if rdr == nil {
		err = ErrInvalidUnpackerParams
		return
	}
	up = &Unpacker{
		rdr: rdr,
		id:  id,
	}
	up.ctx, up.cf = context.WithCancel(context.Background())
	return
}

func (up *Unpacker) Cancel() {
	if up.cf != nil {
		up.cf()
	}
}

func (up *Unpacker) Unpack(uph UnpackHandler) (err error) {
	//check parameters
	var hdr *tar.Header
	if uph == nil {
		err = ErrInvalidUnpackerParams
		return
	}

	rdr := contextio.NewReader(up.ctx, up.rdr)
	//wire up our readers
	var zrdr io.ReadCloser
	if zrdr, err = zlib.NewReader(rdr); err != nil {
		return
	}
	trdr := tar.NewReader(zrdr)
	for {
		if hdr, err = trdr.Next(); err == io.EOF {
			err = nil
			break
		} else if err != nil {
			break
		} else if hdr.Typeflag != tar.TypeReg {
			err = ErrInvalidFileType
			break
		}
		//if this is a tag update, update the tags instead
		if hdr.Name == tagupdateFilename {
			if err = up.updateTags(trdr, uph); err != nil {
				break
			}
			continue
		}

		var ft Ftype
		if ft, err = FilenameToType(hdr.Name); err != nil {
			return
		} else if err = up.hitType(ft); err != nil {
			return
		}
		//copy from the tar file to our context writer wrapped file handle
		if err = uph.HandleFile(ft.Filepath(up.id), contextio.NewReader(up.ctx, trdr)); err != nil {
			break
		}
	}
	if up.cf != nil {
		up.cf()
	}
	if err == nil {
		err = up.allFilesHit(false) //we are NOT being strict
	}
	return
}

func (up *Unpacker) updateTags(trdr io.Reader, uph UnpackHandler) (err error) {
	var ts []tags.TagPair
	//decode the tagset
	if err = gob.NewDecoder(trdr).Decode(&ts); err != nil {
		return
	}
	if err = uph.HandleTagUpdate(ts); err != nil {
		return
	}
	return up.hitType(TagsUpdate)
}

func trimVersion(nm string) string {
	return strings.TrimSuffix(nm, filepath.Ext(nm))
}
