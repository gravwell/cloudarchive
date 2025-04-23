/*************************************************************************
 * Copyright 2025 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

// Package client package is used for interacting with remote cloudarchive server implemenations
// specific server implemenations may implement additonal APIs, this client covers the
// basic APIs required to interact with the open source server implementations.
package client

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gravwell/cloudarchive/pkg/shardpacker"
	"github.com/gravwell/cloudarchive/pkg/tags"
	"github.com/gravwell/cloudarchive/pkg/util"
	"github.com/gravwell/cloudarchive/pkg/webserver"
)

const (
	maxRedirects    = 3
	methodLogoutAll = `DELETE`
	methodLogout    = `PUT`

	defaultUserAgent = `GravwellCloudArchiveClient`
	authHeaderName   = `Authorization`
	cntType          = `GravwellShard`
)

var (
	errNoRedirect error = nil

	ErrInvalidTestStatus error = errors.New("Invalid status on webserver test")
	ErrAccountLocked     error = errors.New(`Account is Locked`)
	ErrLoginFail         error = errors.New(`Username and Password are incorrect`)
	ErrNotSynced         error = errors.New(`Client has not been synced`)
	ErrNoLogin           error = errors.New("Not logged in")

	tickChunkSize = 128 * 1024      //tick every 32KB
	tickTimeout   = 8 * time.Second //basically we have to maintain 32KB/s
	testTimeouts  = time.Second
)

type Client struct {
	server      string
	serverURL   *url.URL
	clnt        *http.Client
	mtx         *sync.Mutex
	state       clientState
	lastNotifId uint64
	enforceCert bool
	headerMap   map[string]string //additional header values to add to requests
	sessionData ActiveSession
	wsScheme    string
	httpScheme  string
	tlsConfig   *tls.Config
	transport   *http.Transport
	custID      uint64
}

type ActiveSession struct {
	JWT                string
	LastNotificationID uint64
}

// NewClient creates a new client targeted at server.
// enforceCertificate allows for self-signed certs
func NewClient(server string, enforceCertificate, useHttps bool) (*Client, error) {
	var wsScheme string
	var httpScheme string
	var tlsConfig *tls.Config
	if server == "" {
		return nil, errors.New("invalid base URL")
	}
	if useHttps {
		wsScheme = `wss`
		httpScheme = `https`
		tlsConfig = &tls.Config{InsecureSkipVerify: !enforceCertificate}
	} else {
		wsScheme = `ws`
		httpScheme = `http`
	}
	serverURL, err := url.Parse(fmt.Sprintf("%s://%s", httpScheme, server))
	if err != nil {
		return nil, err
	}

	//setup a transport that allows a bad client if the user asks for it
	tr := &http.Transport{
		TLSClientConfig: tlsConfig,
		MaxIdleConns:    1,
		IdleConnTimeout: 30 * time.Second,
		MaxConnsPerHost: 2,
	}
	clnt := http.Client{
		Transport:     tr,
		CheckRedirect: redirectPolicy, //use default redirect policy
	}
	//create the header map and stuff our user-agent in there
	hdrMap := make(map[string]string, 1)
	hdrMap[`User-Agent`] = defaultUserAgent

	//actually build and return the client
	return &Client{
		server:      server,
		serverURL:   serverURL,
		clnt:        &clnt,
		mtx:         &sync.Mutex{},
		state:       STATE_NEW,
		enforceCert: enforceCertificate,
		headerMap:   hdrMap,
		wsScheme:    wsScheme,
		httpScheme:  httpScheme,
		tlsConfig:   tlsConfig,
		transport:   tr,
	}, nil
}

// we allow a single redirect to allow for the muxer to clean up requests
// basically the gorilla muxer we are using will force a 301 redirect on a path
// such as '//' to '/'  We allow for one of those
func redirectPolicy(req *http.Request, via []*http.Request) error {
	if len(via) >= 2 {
		return errors.New("Disallowed multiple redirects")
	} else if len(via) == 1 {
		if path.Clean(req.URL.Path) == path.Clean(via[0].URL.Path) {
			//ensure that any set headers are transported forward
			lReq := via[len(via)-1]
			for k, v := range lReq.Header {
				_, ok := req.Header[k]
				if !ok {
					req.Header[k] = v
				}
			}
			return nil
		}
		return errors.New("Disallowed non-equivelent redirects")
	}
	return errors.New("Uknown redirect chain")
}

// Test checks if the specified webserver is functioning
func (c *Client) Test() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, TEST_URL)
	c.clnt.Timeout = testTimeouts
	resp, err := c.clnt.Get(uri)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return ErrInvalidTestStatus
	}
	return nil
}

func (c *Client) SetUserAgent(val string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if val == `` {
		val = defaultUserAgent
	}
	c.headerMap[`User-Agent`] = val
}

// TestLogin checks if we're logged in to the webserver
func (c *Client) TestLogin() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.clnt.Timeout = testTimeouts
	return c.getStaticURL(TEST_AUTH_URL, nil)
}

// Login logs into the URL and grabs the jwt
func (c *Client) Login(user, pass string) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.state != STATE_NEW && c.state != STATE_LOGGED_OFF {
		return errors.New("Client not ready for login")
	}
	if user == "" {
		return errors.New("Invalid username")
	}
	cid, err := strconv.ParseUint(user, 10, 64)
	if err != nil {
		return err
	}

	//build up URL we are going to throw at
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, LOGIN_URL)

	//build up the form that we are going to throw at login url
	loginCreds := url.Values{}
	loginCreds.Add(USER_FIELD, user)
	loginCreds.Add(PASS_FIELD, pass)

	//build up the request
	req, err := http.NewRequest(`POST`, uri, strings.NewReader(loginCreds.Encode()))
	if err != nil {
		return err
	}
	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	req.Header.Set(`Content-Type`, `application/x-www-form-urlencoded`)

	//post the form to the base login url
	c.clnt.Timeout = testTimeouts
	resp, err := c.clnt.Do(req)
	if err != nil {
		return err
	}
	//check response
	if resp == nil {
		//this really should never happen
		return errors.New("Invalid response")
	}
	defer resp.Body.Close()

	//look for the redirect response
	switch resp.StatusCode {
	case http.StatusLocked:
		return ErrAccountLocked
	case http.StatusUnprocessableEntity:
		return ErrLoginFail
	case http.StatusOK:
	default:
		return fmt.Errorf("Invalid response: %d", resp.StatusCode)
	}

	var loginResp webserver.LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		return err
	}
	err = c.processLoginResponse(loginResp)
	if err == nil {
		c.custID = cid
	}
	return err
}

func (c *Client) processLoginResponse(loginResp webserver.LoginResponse) error {
	//check that we had a good login
	if !loginResp.LoginStatus {
		return errors.New(loginResp.Reason)
	}

	//double check that we have the JWT
	if loginResp.JWT == "" {
		return errors.New("Failed to retrieve JWT")
	}

	//save away our tokens in our header map, which will be injected into requests
	c.headerMap[authHeaderName] = fmt.Sprintf("Bearer %s", loginResp.JWT)

	c.sessionData = ActiveSession{
		JWT: loginResp.JWT,
	}

	c.state = STATE_AUTHED
	return nil
}

func (c *Client) PullTags(guid string) (tset []tags.TagPair, err error) {
	err = c.getStaticURL(fmt.Sprintf("/api/tags/%d/%s", c.custID, guid), &tset)
	return
}

func (c *Client) SyncTags(guid string, idxTags []tags.TagPair) (tset []tags.TagPair, err error) {
	err = c.postStaticURL(fmt.Sprintf("/api/tags/%d/%s", c.custID, guid), idxTags, &tset)
	return
}

func (c *Client) ListIndexers() ([]string, error) {
	var r []string
	err := c.getStaticURL(fmt.Sprintf("/api/shard/%d", c.custID), &r)
	return r, err
}

func (c *Client) ListIndexerWells(guid string) ([]string, error) {
	var r []string
	url := fmt.Sprintf("/api/shard/%d/%s", c.custID, guid)
	err := c.getStaticURL(url, &r)
	return r, err
}

func (c *Client) GetWellTimeframe(guid, well string) (util.Timeframe, error) {
	var r util.Timeframe
	url := fmt.Sprintf("/api/shard/%d/%s/%s", c.custID, guid, well)
	err := c.getStaticURL(url, &r)
	return r, err
}

func (c *Client) GetWellShardsInTimeframe(guid, well string, tf util.Timeframe) ([]string, error) {
	var r []string
	url := fmt.Sprintf("/api/shard/%d/%s/%s", c.custID, guid, well)
	err := c.postStaticURL(url, tf, &r)
	return r, err
}

func (c *Client) PushShard(sid ShardID, spath string, tps []tags.TagPair, tags []string, ctx context.Context) error {
	pkr := shardpacker.NewPacker(sid.Shard)
	trdr, err := newReadTicker(pkr, tickChunkSize)
	if err != nil {
		return err
	}
	c.clnt.Timeout = 0
	ctx, cf := context.WithCancel(ctx)
	defer cf()
	reqRespChan := make(chan error, 1)
	go c.asyncPushShard(sid, trdr, ctx, reqRespChan)
	packChan := make(chan error, 1)
	go c.asyncPackShard(spath, tps, tags, pkr, packChan)

	tckr := trdr.ticker()
	tmr := time.NewTimer(tickTimeout)

	//its possible to cancel or timeout
	//so we fire off the request in the background and then watch for:
	// 1. cancellation via the cancel channel
tickLoop:
	for {
		select {
		case err = <-packChan:
			if err == nil {
				err = <-reqRespChan //wait for the request to finish then bail
			} else {
				<-reqRespChan // pack chan errored out, get the request to finish and discard error
			}
			break tickLoop
		case err = <-reqRespChan:
			if err == nil {
				err = <-packChan //check the packer error
			} else {
				pkr.Cancel()
				<-packChan //ignore the packer error
			}
			break tickLoop
		case <-tckr:
			tmr.Reset(tickTimeout) //and continue
		case <-tmr.C:
			err = errors.New("upload timeout")
			//cancel both contexts
			pkr.Cancel()
			cf()
			//discard the error, we re reporting the timeout
			<-reqRespChan
			<-packChan //ignore the packer error
			break tickLoop
		case <-ctx.Done():
			pkr.Cancel()
			//we should get an error about cancellation
			<-packChan //ignore the packer error
			err = <-reqRespChan
			break tickLoop
		}
	}
	close(reqRespChan)
	close(packChan)
	return err
}

// asyncPushShard is a background method that actually performs the HTTP request
// it will execute the request and copy from the rdr to the http request
// results are returned via the rchan parameter
func (c *Client) asyncPushShard(sid ShardID, rdr io.Reader, ctx context.Context, rchan chan error) {
	resp, err := c.methodRequestURLWithContext(http.MethodPost, sid.PushShardUrl(c.custID), cntType, rdr, ctx)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("Bad Status %s(%d): %s", resp.Status, resp.StatusCode, getBodyErr(resp.Body))
	}
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	rchan <- err
}

// packShard processes a complete shard, pushsing each component into the
// shardpacker.Packer object (a compressed tarball)
func (c *Client) asyncPackShard(spath string, tps []tags.TagPair, tgs []string, pkr *shardpacker.Packer, rchan chan error) {
	id := filepath.Base(spath)

	if err := pkr.AddTags(tps); err != nil {
		rchan <- err
		pkr.CloseWithError(err)
		return
	}
	if err := pkr.AddWellTags(tgs); err != nil {
		rchan <- err
		pkr.CloseWithError(err)
		return
	}
	if err := util.AddShardFilesToPacker(spath, id, pkr); err != nil {
		rchan <- err
		pkr.CloseWithError(err)
	} else {
		rchan <- pkr.Close() //send final potential error
	}
}

func (c *Client) PullShard(sid ShardID, spath string, cancel context.Context) error {
	//make the request and get the body
	ctx, cf := context.WithCancel(context.Background())
	defer cf()
	c.clnt.Timeout = 0
	resp, err := c.methodRequestURLWithContext(http.MethodGet, sid.PushShardUrl(c.custID), ``, nil, ctx)
	if err != nil {
		return err
	} else if err == nil && resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("Bad Status %s(%d): %s", resp.Status, resp.StatusCode, getBodyErr(resp.Body))
	}
	defer resp.Body.Close()

	trdr, err := newReadTicker(resp.Body, tickChunkSize)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(spath, 0770); err != nil {
		return err
	}
	upkr, err := shardpacker.NewUnpacker(sid.Shard, trdr)
	if err != nil {
		return err
	}
	reqRespChan := make(chan error, 1)
	go c.asyncUnpackShard(spath, upkr, reqRespChan)

	tckr := trdr.ticker()
	tmr := time.NewTimer(tickTimeout)

	//its possible to cancel or timeout
	//so we fire off the request in the background and then watch for:
	// 1. cancellation via the cancel channel
tickLoop:
	for {
		select {
		case err = <-reqRespChan:
			if err != nil {
				upkr.Cancel()
			}
			break tickLoop
		case <-tckr:
			tmr.Reset(tickTimeout) //and continue
		case <-tmr.C:
			err = errors.New("download timeout")
			//cancel both contexts
			cf()
			upkr.Cancel()
			//discard the error, we re reporting the timeout
			<-reqRespChan
			break tickLoop
		case <-cancel.Done():
			cf()
			upkr.Cancel()
			//we should get an error about cancellation
			err = <-reqRespChan
			break tickLoop
		}
	}
	close(reqRespChan)
	return err
}

type unpackHandler struct {
	base string
}

func (c *Client) asyncUnpackShard(spath string, upkr *shardpacker.Unpacker, rchan chan error) {
	uph := unpackHandler{
		base: filepath.Clean(spath),
	}
	rchan <- upkr.Unpack(uph)
}

func (uh unpackHandler) HandleTagUpdate([]tags.TagPair) error {
	return nil //ignore it
}

func (uh unpackHandler) HandleFile(p string, rdr io.Reader) error {
	if p = filepath.Clean(p); p == `` || p == `.` {
		return errors.New("Invalid filename")
	}
	//check if we need to make a directory
	if d, _ := filepath.Split(p); d != `` {
		if err := os.MkdirAll(filepath.Join(uh.base, d), 0770); err != nil {
			return err
		}
	}
	fout, err := os.OpenFile(filepath.Clean(filepath.Join(uh.base, p)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0660)
	if err != nil {
		return err
	}

	if _, err := io.Copy(fout, rdr); err != nil {
		fout.Close()
		return err
	}

	return fout.Close() //ignore it
}
