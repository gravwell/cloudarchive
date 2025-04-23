/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package tags

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v4/ingest/entry"
)

var (
	baseDir string
	tagFile string
)

func TestMain(m *testing.M) {
	var err error
	if baseDir, err = os.MkdirTemp(os.TempDir(), "gravtags"); err != nil {
		log.Fatal(err)
	}
	tagFile = filepath.Join(baseDir, `tags.dat`)
	r := m.Run()
	if err := os.RemoveAll(baseDir); err != nil {
		log.Fatal(err)
	}
	os.Exit(r)
}

func TestInit(t *testing.T) {
	//open and close twice to check flocks
	if tm, err := New(tagFile); err != nil {
		t.Fatal(err)
	} else if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	if tm, err := New(tagFile); err != nil {
		t.Fatal(err)
	} else if err := tm.Close(); err != nil {
		t.Fatal(err)
	}

	//open twice to check that flock prevents it
	if tm, err := New(tagFile); err != nil {
		t.Fatal(err)
	} else if err := tm.Close(); err != nil {
		t.Fatal(err)
	}

}

func TestAdd(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 0xff; i++ {
		if err = tm.AddTag(fmt.Sprintf("namedtag%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestImport(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	var tgs []string
	for i := 0xf0; i < 0x1ff; i++ {
		tgs = append(tgs, fmt.Sprintf("namedtag%d", i))
	}
	if err = tm.ImportTags(tgs); err != nil {
		t.Fatal(err)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetAndPopulate(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0x1ff; i < 0x2ff; i++ {
		if _, err = tm.GetAndPopulate(fmt.Sprintf("namedtag%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRetrieve(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 0x2ff; i++ {
		tg, err := tm.GetTag(fmt.Sprintf("namedtag%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if tg == 0 {
			t.Fatal("Returned default tag when it shouldn't have")
		}
	}

	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTagmap(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	tg, err := tm.GetAndPopulate("thisisatest")
	if err != nil {
		t.Fatal(err)
	}
	s, err := tm.ReverseLookup(tg)
	if err != nil {
		t.Fatal(err)
	}
	if s != "thisisatest" {
		t.Fatalf("Did not get the right tag back: %v != thisisatest", s)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTagmapReset(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	tg, err := tm.GetTag("thisisatest")
	if err != nil {
		t.Fatal(err)
	}
	s, err := tm.ReverseLookup(tg)
	if err != nil {
		t.Fatal(err)
	}
	if s != "thisisatest" {
		t.Fatalf("Did not get the right tag back: %v != thisisatest", s)
	}
	//reset it
	set := []TagPair{{Name: `thisisatest`, Value: 1}}
	if err = tm.ResetOverride(set); err != nil {
		t.Fatal(err)
	}
	tg2, err := tm.GetTag(s)
	if err != nil {
		t.Fatal(err)
	}
	if tg == tg2 {
		t.Fatal("Tagmanager did not take on new set")
	}
	s2, err := tm.ReverseLookup(tg2)
	if err != nil {
		t.Fatal(err)
	}
	if s2 != s {
		t.Fatal("Tagmanager reset invalid ", s, s2)
	}
	//close and reopen
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	if tm, err = New(tagFile); err != nil {
		t.Fatal(err)
	}
	if tg, err = tm.GetTag("thisisatest"); err != nil {
		t.Fatal(err)
	}
	if s, err = tm.ReverseLookup(tg); err != nil {
		t.Fatal(err)
	}
	if s != "thisisatest" {
		t.Fatalf("Did not get the right tag back: %v != thisisatest", s)
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTagmapMerge(t *testing.T) {
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	tg, err := tm.GetTag("thisisatest")
	if err != nil {
		t.Fatal(err)
	}
	s, err := tm.ReverseLookup(tg)
	if err != nil {
		t.Fatal(err)
	}
	if s != "thisisatest" {
		t.Fatalf("Did not get the right tag back: %v != thisisatest", s)
	}
	//merge some new tags it
	set := []TagPair{{Name: `chucktesta`, Value: 99}}
	var updated bool
	if updated, err = tm.Merge(set); err != nil {
		t.Fatal(err)
	} else if !updated {
		t.Fatal("update did not happen")
	}
	tg2, err := tm.GetTag(s)
	if err != nil {
		t.Fatal(err)
	}
	if tg != tg2 {
		t.Fatal("Tagmanager merge corrupted originals")
	}
	s2, err := tm.ReverseLookup(tg2)
	if err != nil {
		t.Fatal(err)
	}
	if s2 != s {
		t.Fatal("Tagmanager reset invalid ", s, s2)
	}

	if tg, err = tm.GetTag(`chucktesta`); err != nil {
		t.Fatal(err)
	}
	if tg != 99 {
		t.Fatal("Merged tag set did not merge correctly")
	}
	//merge with something that should NOT update
	if updated, err = tm.Merge(set); err != nil {
		t.Fatal(err)
	} else if updated {
		t.Fatal("updated after second merge")
	}

	//close and reopen
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
	if tm, err = New(tagFile); err != nil {
		t.Fatal(err)
	}
	//merge with a bad set
	set = []TagPair{{Name: `chucktesta`, Value: 199}}
	if _, err = tm.Merge(set); err == nil {
		t.Fatal("Failed to catch bad merge")
	}
	if tg, err = tm.GetTag(`chucktesta`); err != nil {
		t.Fatal(err)
	} else if tg != 99 {
		t.Fatal("merge corrupted tag set")
	}
	if err := tm.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTagSubset(t *testing.T) {
	var tags []entry.EntryTag
	tm, err := New(tagFile)
	if err != nil {
		t.Fatal(err)
	}
	tg, err := tm.GetTag("thisisatest")
	if err != nil {
		t.Fatal(err)
	}
	tags = []entry.EntryTag{tg}
	mp, err := tm.GetTagSubmap(tags)
	if err != nil {
		t.Fatal(err)
	}
	if len(mp) != 1 {
		t.Fatal("returned subset wrong ", len(mp))
	}
	if mp[`thisisatest`] != tg {
		t.Fatal("Resulting subset is wrong ", tg)
	}
}

func ManagerGet(t *testing.T) {
	for i := 0; i < 16; i++ {
		id := rand.Uint64()
		guid := uuid.New()
		pth := filepath.Join(baseDir, fmt.Sprintf("%d", id))
		if err := os.Mkdir(pth, 0770); err != nil {
			t.Fatal(err)
		}
		if _, err := GetTagMan(id, guid, pth); err != nil {
			t.Fatal(err)
		}
		if err := ReleaseTagMan(id, guid); err != nil {
			t.Fatal(err)
		}
	}
	if err := CloseTagSets(); err != nil {
		t.Fatal(err)
	}
}

func ManagerGetMultiple(t *testing.T) {
	var ids []uint64
	var guids []uuid.UUID
	for i := 0; i < 16; i++ {
		id := rand.Uint64()
		guid := uuid.New()
		pth := filepath.Join(baseDir, fmt.Sprintf("%d", id))
		if err := os.Mkdir(pth, 0770); err != nil {
			t.Fatal(err)
		}
		//grab it twice
		if _, err := GetTagMan(id, guid, pth); err != nil {
			t.Fatal(err)
		}
		if _, err := GetTagMan(id, guid, pth); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		guids = append(guids, guid)
	}

	//attemp to close the set, should error
	if err := CloseTagSets(); err == nil {
		t.Fatal("allowed to close with active handles")
	}

	for i := range ids {
		//release two times (both should succeed
		if err := ReleaseTagMan(ids[i], guids[i]); err != nil {
			t.Fatal(err)
		}
		if err := ReleaseTagMan(ids[i], guids[i]); err != nil {
			t.Fatal(err)
		}
		//third time should fail
		if err := ReleaseTagMan(ids[i], guids[i]); err == nil {
			t.Fatal("Allowed to over release")
		}
	}
	//this time should succeed
	if err := CloseTagSets(); err != nil {
		t.Fatal(err)
	}
}
