/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package shardpacker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gravwell/cloudarchive/pkg/tags"

	"github.com/google/uuid"
)

var (
	tdir    string
	idxdir  string
	defWell string = `default`
	idxguid uuid.UUID
)

func TestMain(m *testing.M) {
	var err error
	tdir, err = ioutil.TempDir(os.TempDir(), "gravpack")
	if err != nil {
		log.Fatal(err)
	}
	idxguid = uuid.New()
	idxdir = filepath.Join(tdir, idxguid.String())
	r := m.Run()
	if err := os.RemoveAll(tdir); err != nil {
		log.Fatal(err)
	}
	os.Exit(r)
}

func TestNew(t *testing.T) {
	id := `deadbeef01`
	p := NewPacker(id)
	//with no reader, this will throw an error
	if err := p.CloseWithTimeout(100 * time.Millisecond); err == nil {
		t.Fatal("failed to catch timeout")
	} else if err = p.Close(); err == nil {
		t.Fatal("Failed to catch already closed")
	}
	//run with a discarder, close will finish
	p = NewPacker(id)
	go io.Copy(ioutil.Discard, p)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	if up, err := NewUnpacker(id, bytes.NewBuffer(nil)); err != nil {
		t.Fatal(err)
	} else {
		up.Cancel()
	}
}

type ftest struct {
	tp Ftype
	v  string
}

func TestPack(t *testing.T) {
	id := `deadbeef02`
	p := NewPacker(id)
	go io.Copy(ioutil.Discard, p) //just discarding

	tsts := []ftest{
		ftest{tp: Store, v: `store`},
		ftest{tp: Index, v: `index`},
		ftest{tp: Verify, v: `verify`},
		ftest{tp: AccelFile, v: `accelerator`},
	}

	//everything should work
	for _, v := range tsts {
		bb := bytes.NewBuffer([]byte(v.v))
		if err := p.AddFile(v.tp, int64(bb.Len()), bb); err != nil {
			t.Fatal(err)
		}
	}

	//check for not catching a repeat
	bb := bytes.NewBuffer([]byte(`a test`))
	sz := int64(bb.Len())
	if err := p.AddFile(Store, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.AddFile(Index, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.AddFile(Verify, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.AddFile(AccelFile, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.AddFile(IndexAccelKeyFile, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.AddFile(IndexAccelDataFile, sz, bb); err == nil {
		t.Fatal("failed to catch repeat")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPackUnpackNoAccel(t *testing.T) {
	id := `deadbeef03`
	tsts := []ftest{
		ftest{tp: Store, v: `store`},
		ftest{tp: Index, v: `index`},
		ftest{tp: Verify, v: `verify`},
	}
	if err := testCycle(id, tsts); err != nil {
		t.Fatal(err)
	}
}

func TestPackUnpackAccelFile(t *testing.T) {
	id := `deadbeef04`
	tsts := []ftest{
		ftest{tp: Store, v: `store`},
		ftest{tp: Index, v: `index`},
		ftest{tp: Verify, v: `verify`},
		ftest{tp: AccelFile, v: `accelerator`},
	}
	if err := testCycle(id, tsts); err != nil {
		t.Fatal(err)
	}
}

func TestPackUnpackIndexAccel(t *testing.T) {
	id := `deadbeef05`
	tsts := []ftest{
		ftest{tp: Store, v: `store`},
		ftest{tp: Index, v: `index`},
		ftest{tp: Verify, v: `verify`},
		ftest{tp: IndexAccelKeyFile, v: `keystuff`},
		ftest{tp: IndexAccelDataFile, v: `datastuff`},
	}
	if err := testCycle(id, tsts); err != nil {
		t.Fatal(err)
	}
}

func testCycle(id string, tsts []ftest) error {
	sdir, err := genUnpackDirs(id)
	if err != nil {
		return err
	}
	cid, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		return err
	}

	tuh := testUnpackHandler{
		sdir: sdir,
		cid:  cid,
	}

	rch := make(chan error, 1)
	p := NewPacker(id)
	up, err := NewUnpacker(id, p)
	if err != nil {
		return err
	}
	go func() {
		//unpacker is discarding
		err := up.Unpack(tuh)
		if err != nil {
			fmt.Println("Unpack error", err)
		}
		rch <- err
	}()

	//add the tags update
	tu := []tags.TagPair{
		tags.TagPair{Name: `test`, Value: 1},
		tags.TagPair{Name: `test2`, Value: 2},
	}
	if err := p.AddTags(tu); err != nil {
		return err
	}
	wellTags := []string{`test`, `test2`}
	if err := p.AddWellTags(wellTags); err != nil {
		return err
	}

	//everything should work
	for _, v := range tsts {
		bb := bytes.NewBuffer([]byte(v.v))
		if err := p.AddFile(v.tp, int64(bb.Len()), bb); err != nil {
			return err
		}
	}
	if err := p.Flush(); err != nil {
		return err
	}
	if err := p.Close(); err != nil {
		return err
	}
	if err := <-rch; err != nil {
		return err
	}
	//check the files
	for _, v := range tsts {
		fpth := filepath.Join(sdir, v.tp.Filepath(id))
		if cnt, err := ioutil.ReadFile(fpth); err != nil {
			return err
		} else if string(cnt) != v.v {
			return fmt.Errorf("Bad contents: %v != %v\n", string(cnt), v.v)
		}
	}
	return nil
}

func TestAbort(t *testing.T) {
	id := `feedfebe00`
	sdir, err := genUnpackDirs(id)
	if err != nil {
		t.Fatal(err)
	}
	cid, err := strconv.ParseUint(id, 16, 64)
	if err != nil {
		t.Fatal(err)
	}

	tuh := testUnpackHandler{
		sdir: sdir,
		cid:  cid,
	}

	rch := make(chan error, 1)
	p := NewPacker(id)
	up, err := NewUnpacker(id, p)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		rch <- up.Unpack(tuh)
	}()

	//push in a store file
	bb := bytes.NewBuffer([]byte("stuff"))
	if err := p.AddFile(Store, int64(bb.Len()), bb); err != nil {
		t.Fatal(err)
	}
	if err := p.CloseWithError(errors.New("testing")); err != nil {
		t.Fatal(err)
	}

	if err := <-rch; err == nil {
		t.Fatal("did not catch the abort")
	}
}

func genUnpackDirs(id string) (pth string, err error) {
	pth = filepath.Join(idxdir, defWell, id)
	err = os.MkdirAll(pth, 0770)
	return
}

type testUnpackHandler struct {
	bdir string
	sdir string
	cid  uint64
}

func (tuh testUnpackHandler) HandleFile(fname string, rdr io.Reader) error {
	if d, _ := filepath.Split(fname); len(d) != 0 {
		err := os.Mkdir(filepath.Join(tuh.sdir, d), 0770)
		if err != nil && !os.IsExist(err) {
			return err
		}
	}
	fout, err := os.Create(filepath.Join(tuh.sdir, fname))
	if err != nil {
		return err
	}
	if _, err := io.Copy(fout, rdr); err != nil {
		fout.Close()
		return err
	}
	return fout.Close()
}

func (tuh testUnpackHandler) HandleTagUpdate([]tags.TagPair) error {
	return nil
}
