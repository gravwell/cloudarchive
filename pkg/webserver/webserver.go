/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package webserver

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	golog "log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/gravwell/gravwell/v3/ingest/log"
)

const (
	LOGIN_PATH     string = "/api/login"
	TEST_PATH      string = "/api/test"
	AUTH_TEST_PATH string = "/api/testauth"
	SHARD_PATH     string = "/api/shard/{custid}/{uuid}/{well}/{shardid}"
	CUST_PATH      string = "/api/shard/{custid}"
	INDEXER_PATH   string = "/api/shard/{custid}/{uuid}"
	WELL_PATH      string = "/api/shard/{custid}/{uuid}/{well}"
	TAG_PATH       string = "/api/tags/{custid}/{uuid}"
)

type Webserver struct {
	m            *mux.Router
	tlsConfig    *tls.Config
	lst          *net.Listener
	listenString string
	exitError    chan error
	lgr          *log.Logger
	authModule   Authenticator
	shardHandler ShardHandler

	hmacSecret []byte

	initialized bool
	running     bool
}

type WebserverConfig struct {
	ListenString string // addr:port
	DisableTLS   bool
	CertFile     string
	KeyFile      string
	Logger       *log.Logger
	ShardHandler ShardHandler
	Auth         Authenticator
}

func NewWebserver(conf WebserverConfig) (*Webserver, error) {
	var err error
	var config *tls.Config
	if !conf.DisableTLS {
		config = &tls.Config{
			MinVersion:               tls.VersionTLS12,
			PreferServerCipherSuites: true,
			CipherSuites: []uint16{
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
				tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			},
		}
		if config.NextProtos == nil {
			config.NextProtos = []string{"http/1.1"}
		}

		config.Certificates = make([]tls.Certificate, 1)
		config.Certificates[0], err = tls.LoadX509KeyPair(conf.CertFile, conf.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	routineExitChan := make(chan error, 2)
	ws := &Webserver{
		tlsConfig:    config,
		listenString: conf.ListenString,
		exitError:    routineExitChan,
		lgr:          conf.Logger,
		shardHandler: conf.ShardHandler,
		authModule:   conf.Auth,
	}

	ws.hmacSecret = make([]byte, 16)
	_, err = rand.Read(ws.hmacSecret)
	if err != nil {
		return nil, err
	}

	if err = ws.buildRequestRouter(); err != nil {
		return nil, err
	}

	return ws, nil
}

func (w *Webserver) Init() error {
	if w.initialized {
		return errors.New("Already initialized")
	}

	lst, err := net.Listen("tcp", w.listenString)
	if err != nil {
		return err
	}
	w.lst = &lst

	w.initialized = true
	return nil
}

func (w *Webserver) Run() error {
	if w.m == nil {
		return errors.New("webserver muxer is nil")
	}

	//if it wasn't initialized by hand, try it now
	if !w.initialized {
		err := w.Init()
		if err != nil {
			return err
		}
	}
	if w.lst == nil {
		return errors.New("Invalid listener")
	}
	go w.routine()
	return nil
}

func (w *Webserver) routine() {
	w.running = true

	//get a nil logger up that discards everything
	lgr := golog.New(ioutil.Discard, ``, 0)

	// all the handlers should have been registered by now
	srv := &http.Server{
		Handler:  w.m,
		ErrorLog: lgr,
	}
	var err error
	if w.tlsConfig != nil {
		//using TLS listener
		err = srv.Serve(tls.NewListener(*w.lst, w.tlsConfig))
	} else {
		//using non-TLS listener
		err = srv.Serve(*w.lst)
	}

	//we have to basically crash out the http.Serve function by closing the listener
	//so we pick up the error and ignore it
	if err != nil && !strings.HasSuffix(err.Error(), "use of closed network connection") {
		w.exitError <- err
	} else {
		w.exitError <- nil
	}

	w.running = false
}

func (w *Webserver) Close() error {
	var finalError error
	var err error

	//was never running, so lets not worry about it
	if !w.running {
		return nil
	}

	if w.lst != nil {
		err := (*w.lst).Close()
		if err != nil {
			finalError = err
		}
	}

	tmr := time.NewTimer(time.Millisecond * 500)
	defer tmr.Stop()

	select {
	case err = <-w.exitError:
		//all is well
	case <-tmr.C:
		err = errors.New("Close timeout")
	}
	w.lst = nil

	if finalError != nil {
		finalError = fmt.Errorf("%v %v", finalError, err)
	}

	return finalError
}

func (w *Webserver) buildRequestRouter() error {
	w.m = mux.NewRouter()

	w.m.Schemes("https")

	//just logging
	logChain, err := newLogChain(w.logAccess)
	if err != nil {
		return err
	}

	// No logging, just authentication
	// This should be used JUDICIOUSLY
	noLogAuthChain, err := newBaseChain(w.noLogAccess, w.AuthUser)
	if err != nil {
		return err
	}

	//logging, authorization, and validation chain
	authChain, err := newBaseChain(w.logAccess, w.AuthUser)
	if err != nil {
		return err
	}

	//install the test path.  It is not logged nor authenticated
	w.m.HandleFunc(TEST_PATH, w.testHandler).Methods(http.MethodGet)

	// install the auth test path. It is not logged but is authenticated
	w.m.PathPrefix(AUTH_TEST_PATH).Handler(noLogAuthChain.Handler(w.authTestHandler)).Methods(http.MethodGet)

	//install the authentication/login post handler
	w.m.PathPrefix(LOGIN_PATH).Handler(logChain.Handler(w.loginPostPage)).Methods(http.MethodPost)

	// The order of these handlers is IMPORTANT!

	// Handler to get back a list of tags for the indexer
	w.m.PathPrefix(TAG_PATH).Handler(authChain.Handler(w.indexerGetTags)).Methods(http.MethodGet)

	// Handler to let an indexer update its tag set
	w.m.PathPrefix(TAG_PATH).Handler(authChain.Handler(w.indexerSyncTags)).Methods(http.MethodPost)

	// Handler to upload a shard
	w.m.PathPrefix(SHARD_PATH).Handler(authChain.Handler(w.shardPushHandler)).Methods(http.MethodPost)

	// Handler to download a shard
	w.m.PathPrefix(SHARD_PATH).Handler(authChain.Handler(w.shardPullHandler)).Methods(http.MethodGet)

	// Handler to get timeframe contained in a given well
	w.m.PathPrefix(WELL_PATH).Handler(authChain.Handler(w.getWellTimeframe)).Methods(http.MethodGet)

	// Handler to request a list of shards that fall in a timeframe AND exist on the server
	w.m.PathPrefix(WELL_PATH).Handler(authChain.Handler(w.getWellShardsInTimeframe)).Methods(http.MethodPost)

	// Handler to list all wells on an indexer
	w.m.PathPrefix(INDEXER_PATH).Handler(authChain.Handler(w.indexerListWells)).Methods(http.MethodGet)

	// Handler to list a customer's indexers
	w.m.PathPrefix(CUST_PATH).Handler(authChain.Handler(w.customerListIndexers)).Methods(http.MethodGet)

	return nil
}

func (w *Webserver) logAccess(res *trackingResponseWriter, req *http.Request) {
	remoteAddr, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		remoteAddr = req.RemoteAddr
	}
	w.lgr.Info("access",
		log.KV("remote", remoteAddr),
		log.KV("method", req.Method),
		log.KV("url", req.URL.Path),
		log.KV("status", res.status),
		log.KV("useragent", req.UserAgent()))
}

func (w *Webserver) noLogAccess(res *trackingResponseWriter, req *http.Request) {
}

// testHandler is used by the client to check whether the webserver is
// even alive.  It always returns 200, and doesn't do anything with data
// it is ONLY for a preflight check.
func (w *Webserver) testHandler(res http.ResponseWriter, req *http.Request) {
	res.WriteHeader(http.StatusOK)
}

func (w *Webserver) authTestHandler(res http.ResponseWriter, req *http.Request, cust *CustomerDetails) {
	res.WriteHeader(http.StatusOK)
}

func sendObject(w http.ResponseWriter, obj interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(obj); err != nil {
		sendError(w, err, http.StatusInternalServerError)
	}
}

func sendError(w http.ResponseWriter, err error, status int) {
	v := struct {
		Error string
	}{
		Error: err.Error(),
	}
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func getObject(r *http.Request, obj interface{}) error {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(obj); err != nil {
		return err
	}
	return nil
}

func serverFail(res http.ResponseWriter, err error) {
	sendError(res, err, http.StatusInternalServerError)
}

func serverInvalid(res http.ResponseWriter, err error) {
	sendError(res, err, http.StatusBadRequest)
}
