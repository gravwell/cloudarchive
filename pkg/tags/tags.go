/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package tags

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/gravwell/cloudarchive/pkg/flock"

	"github.com/gravwell/gravwell/v4/ingest"
	"github.com/gravwell/gravwell/v4/ingest/entry"
)

type TagManager interface {
	AddTag(name string) error
	ImportTags(tags []string) error
	EnsureTag(id entry.EntryTag, name string) error
	GetAndPopulate(name string) (entry.EntryTag, error)
	GetTag(name string) (entry.EntryTag, error)
	ReverseLookup(tg entry.EntryTag) (string, error)
	TagSet() ([]TagPair, error)
	ResetOverride([]TagPair) error
	GetTagSubmap([]entry.EntryTag) (map[string]entry.EntryTag, error)
	Merge([]TagPair) (bool, error)
	Active() bool
	Count() (int, error)
}

type TagMan struct {
	mtx         sync.Mutex
	tagKeys     map[entry.EntryTag]string
	tags        map[string]entry.EntryTag
	nextTag     entry.EntryTag
	backingFile string
	fout        *os.File
	active      bool
}

type TagPair struct {
	Name  string
	Value entry.EntryTag
}

var (
	ErrNotFound      = errors.New("not found")
	ErrNotActive     = errors.New("not active")
	ErrNoEmptyString = errors.New("Tag cannot be an empty string")
)

const (
	TAG_MANAGER_FILENAME string = "tags.dat"
)

func StaticTagPairs() []TagPair {
	return []TagPair{
		{Name: entry.DefaultTagName, Value: entry.DefaultTagId},
		{Name: entry.GravwellTagName, Value: entry.GravwellTagId},
	}
}

func New(p string) (*TagMan, error) {
	var fout *os.File
	var err error
	var newFile bool
	newFile = false
	fullPath := filepath.Clean(p)
	fi, err := os.Stat(fullPath)
	if err != nil {
		if fout, err = os.Create(fullPath); err != nil {
			return nil, err
		}
		newFile = true
	} else {
		if fi.IsDir() {
			return nil, fmt.Errorf("%s is a directory", fullPath)
		}
		if !fi.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is not a regular file", fullPath)
		}
		if fout, err = os.OpenFile(fullPath, os.O_RDWR, 0660); err != nil {
			return nil, err
		}
	}
	if err = flock.Flock(fout, true); err != nil {
		fout.Close()
		return nil, err
	}
	mp := make(map[string]entry.EntryTag)
	keys := make(map[entry.EntryTag]string)
	if newFile {
		// add in the default tag
		if _, err = fmt.Fprintf(fout, "%s=%d\n", entry.DefaultTagName, entry.DefaultTagId); err != nil {
			flock.Funlock(fout)
			fout.Close()
			return nil, err
		}
		mp[entry.DefaultTagName] = entry.DefaultTagId
		keys[entry.DefaultTagId] = entry.DefaultTagName
		//add in the gravwell tag
		if _, err = fmt.Fprintf(fout, "%s=%d\n", entry.GravwellTagName, entry.GravwellTagId); err != nil {
			flock.Funlock(fout)
			fout.Close()
			return nil, err
		}
		mp[entry.GravwellTagName] = entry.GravwellTagId
		keys[entry.GravwellTagId] = entry.GravwellTagName
	}
	tm := &TagMan{
		tagKeys:     keys,
		tags:        mp,
		backingFile: fullPath,
		fout:        fout,
		mtx:         sync.Mutex{},
		active:      true,
	}
	if err = tm.loadTags(); err != nil {
		flock.Funlock(fout)
		fout.Close()
		return nil, err
	}
	return tm, nil
}

// findNextAvailableTag returns the next available integer tag.
// ** caller should hold the lock
func (tm *TagMan) findNextAvailableTag() (entry.EntryTag, error) {
	for i := entry.EntryTag(1); i < entry.EntryTag(0xffff); i++ {
		if _, ok := tm.tagKeys[i]; !ok {
			return i, nil
		}
	}
	return 0, errors.New("No tags available")
}

// allocateTag finds the next available tag for a name and adds it to the set
// the allocation process also writes the tag out to the file
// NOTICE - this is kind of expensive
// ** caller should hold the lock
func (tm *TagMan) allocateTag(name string) error {
	if err := ingest.CheckTag(name); err != nil {
		return err
	}
	_, err := tm.fout.Seek(0, 2)
	if err != nil {
		return err
	}
	// Make sure the "next tag" is unoccupied, it should be
	if _, ok := tm.tagKeys[tm.nextTag]; ok {
		// There's already something there, try again
		tm.nextTag, err = tm.findNextAvailableTag()
		if err != nil {
			return err
		}
	}
	if _, ok := tm.tagKeys[tm.nextTag]; !ok {
		tm.tagKeys[tm.nextTag] = name
		tm.tags[name] = tm.nextTag
		if _, err = fmt.Fprintf(tm.fout, "%s=%d\n", name, tm.nextTag); err != nil {
			return err
		}
		tm.nextTag++
		return nil
	}
	return errors.New("No tags available")
}

func (tm *TagMan) GetTagSubmap(tags []entry.EntryTag) (mp map[string]entry.EntryTag, err error) {
	var s string
	mp = make(map[string]entry.EntryTag, len(tags))
	for _, t := range tags {
		if s, err = tm.ReverseLookup(t); err != nil {
			return
		}
		mp[s] = t
	}
	return
}

// assignTag adds a given tag to the set, we DO NOT check if the tag already exists.  Those checks
// should have already been performed by the caller
// ** caller should hold the lock
func (tm *TagMan) assignTag(name string, value entry.EntryTag) error {
	if err := ingest.CheckTag(name); err != nil {
		return err
	}
	_, err := tm.fout.Seek(0, 2)
	if err != nil {
		return err
	}

	tm.tagKeys[value] = name
	tm.tags[name] = value
	if _, err = fmt.Fprintf(tm.fout, "%s=%d\n", name, value); err != nil {
		return err
	}
	return nil
}

func (tm *TagMan) Active() bool {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	return tm.active
}

func (tm *TagMan) ImportTags(tgs []string) error {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	for _, v := range tgs {
		v = strings.TrimSpace(v)
		if _, err := tm.getAndPopulateNoLock(v); err != nil {
			return err
		}
	}
	return nil
}

func (tm *TagMan) AddTag(name string) error {
	name = strings.TrimSpace(name)
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	if !tm.active {
		return ErrNotActive
	}
	if _, ok := tm.tags[name]; ok {
		return errors.New("Attempting to allocate present tag")
	}
	if err := tm.allocateTag(name); err != nil {
		return err
	}
	return nil
}

func (tm *TagMan) GetTag(name string) (entry.EntryTag, error) {
	name = strings.TrimSpace(name)
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	if !tm.active {
		return 0, ErrNotActive
	}
	tag, ok := tm.tags[name]
	if !ok {
		return 0, ErrNotFound
	}
	return tag, nil
}

func (tm *TagMan) ReverseLookup(tg entry.EntryTag) (string, error) {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	if !tm.active {
		return "", ErrNotActive
	}

	tagname, ok := tm.tagKeys[tg]
	if !ok {
		return "", ErrNotFound
	}
	return tagname, nil
}

func (tm *TagMan) GetAndPopulate(name string) (entry.EntryTag, error) {
	name = strings.TrimSpace(name)
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	if !tm.active {
		return 0, ErrNotActive
	}
	return tm.getAndPopulateNoLock(name)
}

func (tm *TagMan) getAndPopulateNoLock(name string) (entry.EntryTag, error) {
	//try to get the tag
	tag, ok := tm.tags[name]
	if !ok {
		//if we can't, allocate it
		if err := tm.allocateTag(name); err != nil {
			return 0, err
		}
		//the tag should be there now
		tag, ok = tm.tags[name]
		if !ok {
			return 0, ErrNotFound
		}
	}
	return tag, nil

}

func parseLine(line string) (string, entry.EntryTag, error) {
	bits := strings.Split(line, "=")
	if len(bits) != 2 {
		return "", 0, errors.New("Malformed line")
	}
	tagName := strings.TrimSpace(bits[0])
	tag := strings.TrimSpace(bits[1])
	if tagName == "" || tag == "" {
		return "", 0, errors.New("Malformed line")
	}
	//check the first parameter name
	//try to convert the 2nd parameter to an int
	val, err := strconv.ParseUint(tag, 10, 16)
	if err != nil {
		return "", 0, err
	}
	return tagName, entry.EntryTag(val), nil
}

func (tm *TagMan) EnsureTag(id entry.EntryTag, name string) error {
	return tm.ensureTag(id, name)
}

// ensureTag ensures that the tag and name combo exist in the tag manager exactly as passed in
// this function is typically used for ensuring that the gravwell and default tags haven't been
// omitted or altered
func (tm *TagMan) ensureTag(id entry.EntryTag, name string) error {
	//if neither the name or tag exists, just add and move on
	tagName, nameOk := tm.tagKeys[id]
	tagId, idOk := tm.tags[name]

	if !nameOk && !idOk {
		//neither exist, just add them
		tm.tagKeys[id] = name
		tm.tags[name] = id
		return nil //clean add
	}

	//ensure the tag to name relationship is intact
	if id != tagId || name != tagName {
		return fmt.Errorf("Tag %s is not %x when required", name, id)
	}
	//its in there and the relationship is correct
	return nil
}

func (tm *TagMan) loadTags() error {
	var err error
	var line string
	var k string
	var v entry.EntryTag
	tm.mtx.Lock()
	defer tm.mtx.Unlock()

	tm.active = true
	//loop through files parsing and loading each tag
	rdr := bufio.NewReader(tm.fout)
	for line, err = rdr.ReadString('\n'); err == nil; line, err = rdr.ReadString('\n') {
		if line == "" {
			continue
		}
		k, v, err = parseLine(strings.TrimSpace(line))
		if err != nil {
			return err
		}
		if (v != entry.DefaultTagId && k == entry.DefaultTagName) || (v == entry.DefaultTagId && k != entry.DefaultTagName) {
			return fmt.Errorf("tag \"%s\" MUST be %d: not \"%s=%d\"", entry.DefaultTagName, entry.DefaultTagId, k, v)
		}
		if (v != entry.GravwellTagId && k == entry.GravwellTagName) || (v == entry.GravwellTagId && k != entry.GravwellTagName) {
			return fmt.Errorf("tag \"%s\" MUST be %d: not \"%s=%d\"", entry.GravwellTagName, entry.GravwellTagId, k, v)
		}
		if _, ok := tm.tagKeys[v]; ok {
			return fmt.Errorf("tag id %d already exists", v)
		}
		if _, ok := tm.tags[k]; ok {
			return fmt.Errorf("tag name %s already exists", k)
		}
		tm.tagKeys[v] = k
		tm.tags[k] = v
	}

	//check on the default and gravwell tags
	if err := tm.ensureTag(entry.DefaultTagId, entry.DefaultTagName); err != nil {
		return err
	}
	if err := tm.ensureTag(entry.GravwellTagId, entry.GravwellTagName); err != nil {
		return err
	}
	// find the next available tag
	tm.nextTag, err = tm.findNextAvailableTag()
	if err != nil && err != io.EOF {
		return err
	}
	// find the next available tag
	tm.nextTag, err = tm.findNextAvailableTag()
	return err
}

func (tm *TagMan) TagSet() (pairs []TagPair, err error) {
	tm.mtx.Lock()
	if !tm.active {
		tm.mtx.Unlock()
		err = ErrNotActive
		return
	}
	for k, v := range tm.tags {
		pairs = append(pairs, TagPair{Name: k, Value: v})
	}
	tm.mtx.Unlock()
	return
}

func checkTagSet(s []TagPair) error {
	if len(s) > 0xffff {
		return errors.New("Too many tags specified")
	}
	//scan the tags to ensure that default and gravwell are not set to unacceptable values
	for i := range s {
		switch s[i].Name {
		case entry.DefaultTagName:
			if s[i].Value != entry.DefaultTagId {
				return errors.New("Invalid value for default tag")
			}
		case entry.GravwellTagName:
			if s[i].Value != entry.GravwellTagId {
				return errors.New("Invalid value for gravwell tag")
			}
		}
	}
	return nil
}

// ResetOverride forces the tag manager to completely reset state and treat the provided
// tag set and use the provided tags.  If the provided set does not contain
// "default" and "gravwell" they are automatically added
func (tm *TagMan) ResetOverride(s []TagPair) (err error) {
	//do this WITHOUT the lock held
	if err = checkTagSet(s); err != nil {
		return
	}
	//tag set is ok, proceed with reset
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	if !tm.active {
		err = ErrNotActive
		return
	}

	//delete verything
	tm.tagKeys = make(map[entry.EntryTag]string, len(s))
	tm.tags = make(map[string]entry.EntryTag, len(s))
	//truncate our tags file
	if err = tm.fout.Truncate(0); err != nil {
		return
	}
	if _, err = tm.fout.Seek(0, 0); err != nil {
		return
	}

	//push in gravwell and default
	tm.tags[entry.DefaultTagName] = entry.DefaultTagId
	tm.tagKeys[entry.DefaultTagId] = entry.DefaultTagName
	tm.tags[entry.GravwellTagName] = entry.GravwellTagId
	tm.tagKeys[entry.GravwellTagId] = entry.GravwellTagName
	if _, err = fmt.Fprintf(tm.fout, "%s=%d\n", entry.DefaultTagName, entry.DefaultTagId); err != nil {
		return
	}
	if _, err = fmt.Fprintf(tm.fout, "%s=%d\n", entry.GravwellTagName, entry.GravwellTagId); err != nil {
		return
	}

	//add tags and push them to the file

	for _, v := range s {
		if v.Value == entry.DefaultTagId || v.Value == entry.GravwellTagId {
			continue //skip it
		}
		tm.tags[v.Name] = v.Value
		tm.tagKeys[v.Value] = v.Name
		if _, err = fmt.Fprintf(tm.fout, "%s=%d\n", v.Name, v.Value); err != nil {
			return
		}
	}
	return
}

// Count returns the number of active tags
func (tm *TagMan) Count() (cnt int, err error) {
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	if !tm.active {
		err = ErrNotActive
	} else {
		cnt = len(tm.tags)
	}
	return
}

// Merge attempts to merge the given tag set pair list into the given tag manager
// if two tag names are the same, we check that the tag ids are the same
// if they are not the same, we throw an error
func (tm *TagMan) Merge(s []TagPair) (updated bool, err error) {
	if err = checkTagSet(s); err != nil {
		return
	}
	tm.mtx.Lock()
	defer tm.mtx.Unlock()
	if !tm.active {
		err = ErrNotActive
		return
	}
	for _, v := range s {
		var hit bool
		if cname, ok := tm.tagKeys[v.Value]; ok {
			//ensure the name is the same
			if cname != v.Name {
				err = fmt.Errorf("%s tag exists in current set and does not match provided set", v.Name)
				return
			}
			hit = true
		}
		if ctag, ok := tm.tags[v.Name]; ok {
			if ctag != v.Value {
				err = fmt.Errorf("%s tag name exists in current set and does not match", v.Name)
				return
			}
			hit = true
		}
		if !hit {
			if err = tm.assignTag(v.Name, v.Value); err != nil {
				return
			}
			updated = true
		}
	}
	return
}

func (tm *TagMan) Close() (err error) {
	if tm == nil {
		return
	}
	tm.mtx.Lock()
	tm.active = false
	//unlock the file
	if err = flock.Funlock(tm.fout); err != nil {
		tm.fout.Close()
	} else {
		//clean unlock, set the error to the close code
		err = tm.fout.Close()
	}
	tm.fout = nil
	tm.mtx.Unlock()
	return
}
