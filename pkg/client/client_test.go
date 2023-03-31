/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package client

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gravwell/cloudarchive/pkg/auth"
	"github.com/gravwell/cloudarchive/pkg/filestore"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/webserver"
	"goftp.io/server"
	"goftp.io/server/core"
	"goftp.io/server/driver/file"

	"github.com/google/uuid"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	gravlog "github.com/gravwell/gravwell/v3/ingest/log"
)

const (
	custNum    uint64 = 1337
	hackerNum  uint64 = 420
	custPass   string = "foobar"
	hackerPass string = "haxxor"
	listenAddr string = "localhost:12345"
)

var (
	idxUUID       uuid.UUID
	baseDir       string
	localStoreDir string
	ftpServerDir  string
	serverDir     string
	keyFile       string
	certFile      string
	passwordFile  string

	ws *webserver.Webserver
)

func publicKey(priv interface{}) interface{} {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &k.PublicKey
	case *ecdsa.PrivateKey:
		return &k.PublicKey
	default:
		return nil
	}
}

func pemBlockForKey(priv interface{}) *pem.Block {
	switch k := priv.(type) {
	case *rsa.PrivateKey:
		return &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}
	case *ecdsa.PrivateKey:
		b, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			log.Fatalf("Unable to marshal ECDSA private key: %v", err)
		}
		return &pem.Block{Type: "EC PRIVATE KEY", Bytes: b}
	default:
		return nil
	}
}

func makeX509(keyfile, certfile, hostlist string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return fmt.Errorf("failed to generate private key: %s", err)
	}

	notBefore := time.Now().Add(-24 * time.Hour)
	notAfter := notBefore.Add(12 * time.Hour)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return fmt.Errorf("failed to generate serial number: %s", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Gravwell"},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,
		IsCA:      true,

		BasicConstraintsValid: true,
	}

	hosts := strings.Split(hostlist, ",")
	for _, h := range hosts {
		template.Subject.CommonName = h
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
			template.PermittedDNSDomains = append(template.PermittedDNSDomains, h)
		}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		return fmt.Errorf("Failed to create certificate: %s", err)
	}

	certOut, err := os.Create(certfile)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %s", certfile, err)
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certOut.Close()

	keyOut, err := os.OpenFile(keyfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to open %s for writing: %v", keyfile, err)
	}
	pem.Encode(keyOut, pemBlockForKey(priv))
	keyOut.Close()
	return nil
}

func cleanup() {
	if err := os.RemoveAll(baseDir); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(serverDir); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(localStoreDir); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(ftpServerDir); err != nil {
		log.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	idxUUID = uuid.New()
	var err error
	if baseDir, err = ioutil.TempDir(os.TempDir(), "gravcloud"); err != nil {
		log.Fatal(err)
	}
	if serverDir, err = ioutil.TempDir(os.TempDir(), "gravcloud"); err != nil {
		log.Fatal(err)
	}
	if localStoreDir, err = ioutil.TempDir(os.TempDir(), "gravcloud_ftp_localstore"); err != nil {
		log.Fatal(err)
	}

	keyFile = filepath.Join(baseDir, "key.pem")
	certFile = filepath.Join(baseDir, "cert.pem")
	passwordFile = filepath.Join(baseDir, "passwd")

	// Make the keypair
	if err := makeX509(keyFile, certFile, "localhost"); err != nil {
		log.Fatal(err)
	}

	// Set up a valid customer
	auth, err := auth.NewAuthModule(passwordFile)
	if err != nil {
		cleanup()
		log.Fatal(err)
	}
	if err := auth.AddUser(custNum, custPass, 8); err != nil {
		cleanup()
		log.Fatal(err)
	}
	if err := auth.AddUser(hackerNum, hackerPass, 8); err != nil {
		cleanup()
		log.Fatal(err)
	}

	// Stand up the FTP server
	if ftpServerDir, err = ioutil.TempDir(os.TempDir(), "gravcloud_ftp"); err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(ftpServerDir)
	fperm := core.NewSimplePerm("gravwell", "gravgroup")
	factory := &file.DriverFactory{
		RootPath: ftpServerDir,
		Perm:     fperm,
	}
	fauth := &core.SimpleAuth{
		Name:     "gravwell",
		Password: "testpass",
	}
	ftpServer := server.NewServer(&server.ServerOpts{
		Logger:   &core.DiscardLogger{},
		Auth:     fauth,
		Factory:  factory,
		Port:     2000,
		Hostname: "127.0.0.1",
	})
	go func() {
		ftpServer.ListenAndServe()
	}()

	r := m.Run()

	ftpServer.Shutdown()
	cleanup()
	os.Exit(r)
}

func launchWebserver() error {
	var err error
	lgr := gravlog.New(discarder{})

	handler, err := filestore.NewFilestoreHandler(serverDir)
	if err != nil {
		return err
	}

	conf := webserver.WebserverConfig{
		ListenString: listenAddr,
		CertFile:     certFile,
		KeyFile:      keyFile,
		Logger:       lgr,
		ShardHandler: handler,
	}
	if conf.Auth, err = auth.NewAuthModule(passwordFile); err != nil {
		return err
	}

	ws, err = webserver.NewWebserver(conf)
	if err != nil {
		return err
	}

	err = ws.Init()
	if err != nil {
		return err
	}

	err = ws.Run()
	if err != nil {
		return err
	}
	return nil
}

func TestClientConnect(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Test()
	if err != nil {
		t.Fatal(err)
	}

	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientLogin(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.Test()
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	err = cli.TestLogin()
	if err != nil {
		t.Fatal(err)
	}

	if err := ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientShardPush(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	shardid := `769f2`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}
	tps := []tags.TagPair{
		tags.TagPair{Name: `testing`, Value: 1},
	}
	tags := []string{`testing`}
	cancel := make(chan bool, 1)

	//make a fake shard dir with the
	sdir := filepath.Join(baseDir, shardid)
	if err = makeShardDir(sdir, shardid); err != nil {
		t.Fatal(err)
	}
	if err = cli.PushShard(sid, sdir, tps, tags, cancel); err != nil {
		t.Fatal(err)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientShardPull(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	shardid := `769f2`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}

	sdir := filepath.Join(baseDir, "pull", shardid)
	cancel := make(chan bool, 1)
	if err = cli.PullShard(sid, sdir, cancel); err != nil {
		t.Fatal(err)
	}

	if err := validateShardExists(sdir, shardid); err != nil {
		t.Fatal(err)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func validateShardExists(shardDir, shardID string) (err error) {
	// Now look to see if it showed up
	if !fileExists(shardDir) {
		err = fmt.Errorf("%v does not exist!", shardDir)
		return
	}
	extensions := []string{"index", "verify", "store", "accel", "accel/keys", "accel/data"}
	for _, ext := range extensions {
		if p := filepath.Join(shardDir, shardID+"."+ext); !fileExists(p) {
			err = fmt.Errorf("%v does not exist!", p)
			return
		}
	}
	return
}

// we assume a file/dir "exists" if we can stat it.
func fileExists(path string) bool {
	if _, err := os.Stat(path); err == nil {
		return true // simplified, sure
	}
	return false
}

func TestClientListIndexers(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	// Push a shard
	shardid := `769fc`
	sid := ShardID{
		Indexer: idxUUID,
		Well:    `foo`,
		Shard:   shardid,
	}
	tps := []tags.TagPair{
		tags.TagPair{Name: `testing`, Value: 1},
	}
	tags := []string{`testing`}
	cancel := make(chan bool, 1)

	//make a fake shard dir with the
	sdir := filepath.Join(baseDir, shardid)
	if err = makeShardDir(sdir, shardid); err != nil {
		t.Fatal(err)
	}
	if err = cli.PushShard(sid, sdir, tps, tags, cancel); err != nil {
		t.Fatal(err)
	}

	indexers, err := cli.ListIndexers()
	if err != nil {
		t.Fatal(err)
	}
	if len(indexers) != 1 {
		t.Fatalf("Invalid number of indexers: got %v expected %v", len(indexers), 1)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientListIndexerWells(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	wells, err := cli.ListIndexerWells(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(wells) != 1 {
		t.Fatalf("Invalid number of wells: got %v expected %v", len(wells), 1)
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientGetTimeframe(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	tf, err := cli.GetWellTimeframe(idxUUID.String(), "foo")
	// Ask for the time frame covered by the well "foo"
	if err != nil {
		t.Fatal(err)
	}
	if tf.Start.IsZero() || tf.End.IsZero() {
		t.Fatalf("Got a zero in the timeframe: %v", tf)
	}
	if tf.Start.After(tf.End) {
		t.Fatalf("uhh, end is before start?")
	}

	// Get the list of shards covered by that time frame
	shards, err := cli.GetWellShardsInTimeframe(idxUUID.String(), "foo", tf)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 2 {
		t.Fatalf("Expected 2 shards, got %d", len(shards))
	}

	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientPullTags(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	tagset, err := cli.PullTags(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(tagset) != 3 {
		t.Fatalf("Invalid tagset, expected 3 tags got: %+v\n", tagset)
	}
	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClientSyncTags(t *testing.T) {
	// Start a webserver
	if err := launchWebserver(); err != nil {
		t.Fatal(err)
	}

	// Connect to it
	cli, err := NewClient(listenAddr, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// log in
	err = cli.Login(fmt.Sprintf("%d", custNum), custPass)
	if err != nil {
		t.Fatal(err)
	}

	// Pull the tags
	tagset, err := cli.PullTags(idxUUID.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(tagset) != 3 {
		t.Fatalf("Invalid tagset, expected 3 tags got: %+v\n", tagset)
	}

	// Now add a new tag
	tagset = append(tagset, tags.TagPair{Name: "xyzzy", Value: entry.EntryTag(100)})
	if newset, err := cli.SyncTags(idxUUID.String(), tagset); err != nil {
		t.Fatal(err)
	} else {
		if len(newset) != 4 {
			t.Fatalf("Expected 4 tags in new tag set, got %v", len(newset))
		}
		var ok bool
		for _, t := range newset {
			if t.Name == "xyzzy" {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("Did not find newly-added tag in tag set %v", newset)
		}
	}
	if err = ws.Close(); err != nil {
		t.Fatal(err)
	}
}

func makeShardDir(p, id string) error {
	if err := os.Mkdir(p, 0700); err != nil {
		return err
	}
	//drop index file
	fpath := filepath.Join(p, id+`.index`)
	data := []byte(`index stuff`)
	if err := ioutil.WriteFile(fpath, data, 0660); err != nil {
		return err
	}

	//drop verify file
	fpath = filepath.Join(p, id+`.verify`)
	data = []byte(`verify stuff`)
	if err := ioutil.WriteFile(fpath, data, 0660); err != nil {
		return err
	}

	//drop store file
	fpath = filepath.Join(p, id+`.store`)
	data = []byte(`store stuff`)
	if err := ioutil.WriteFile(fpath, data, 0660); err != nil {
		return err
	}

	//drop accel files
	fpath = filepath.Join(p, id+`.accel`)
	if err := os.MkdirAll(fpath, 0770); err != nil {
		return err
	}
	fpath = filepath.Join(p, id+`.accel`, `data`)
	data = []byte(`accel data`)
	if err := ioutil.WriteFile(fpath, data, 0660); err != nil {
		return err
	}
	fpath = filepath.Join(p, id+`.accel`, `keys`)
	data = []byte(`accel keys`)
	if err := ioutil.WriteFile(fpath, data, 0660); err != nil {
		return err
	}

	return nil
}

type discarder struct {
}

func (d discarder) Close() error {
	return nil
}

func (d discarder) Write(b []byte) (int, error) {
	return len(b), nil
}

type testCancelSource struct {
	io.Reader
}

func (t testCancelSource) Cancel() {}
