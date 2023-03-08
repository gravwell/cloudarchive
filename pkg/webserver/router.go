/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

var (
	ErrInvalidChainArgs = errors.New("invalid base chain arguments")
	ErrVariableNotFound = errors.New("Variable not found in request")
)

type barrierHandlerFunc func(http.ResponseWriter, *http.Request) *CustomerDetails
type handlerFunc func(http.ResponseWriter, *http.Request, *CustomerDetails)
type unauthHandlerFunc func(http.ResponseWriter, *http.Request)
type tailHandler func(*trackingResponseWriter, *http.Request)
type trackingResponseWriter struct {
	w       http.ResponseWriter
	status  int
	changed bool
}

type baseChain struct {
	tc tailHandler
	bf barrierHandlerFunc
}

type logChain struct {
	tc tailHandler
}

func newLogChain(tc tailHandler) (*logChain, error) {
	if tc == nil {
		return nil, ErrInvalidChainArgs
	}
	return &logChain{
		tc: tc,
	}, nil
}

func newBaseChain(tc tailHandler, handler barrierHandlerFunc) (*baseChain, error) {
	if tc == nil || handler == nil {
		return nil, ErrInvalidChainArgs
	}
	return &baseChain{
		tc: tc,
		bf: handler,
	}, nil
}

func (lc *logChain) Handler(handler unauthHandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trw := &trackingResponseWriter{w: w} //default is 200
		handler(trw, r)
		//if a tail call is specified, it is always called at the end if not nil
		if lc.tc != nil {
			lc.tc(trw, r)
		}
	})
}

func (bc *baseChain) Handler(handler handlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trw := &trackingResponseWriter{w: w} //default is 200
		//iterate over our base handlers
		udets := bc.bf(trw, r)
		//if any of the handlers wrote something to the
		//data stream or wrote a response code we break
		if trw.Written() {
			return
		}
		if udets == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		handler(trw, r, udets)
		//if a tail call is specified, it is always called at the end
		//it is not nil
		if bc.tc != nil {
			bc.tc(trw, r)
		}
	})
}

// Header is just a passthrough function to provide header
func (trw trackingResponseWriter) Header() http.Header { return trw.w.Header() }

// Write shuttles writes to the actual ResponseWriter while tracking changes
// used to track when a handler actually writes data
func (trw *trackingResponseWriter) Write(data []byte) (int, error) {
	trw.changed = true
	return trw.w.Write(data)
}

// WriteHeader shuttles data to the actual ResponseWriter while tracking changes
// used to track when handler sets status or updates codes
func (trw *trackingResponseWriter) WriteHeader(code int) {
	trw.changed = true
	trw.status = code
	trw.w.WriteHeader(code)
}

// Written indicates when a handler has changed the content of the response writer
func (trw trackingResponseWriter) Written() bool { return trw.changed }

func (trw trackingResponseWriter) StatusCode() int {
	if trw.status == 0 {
		return 200
	}
	return trw.status
}

func (trw trackingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	var conn net.Conn
	hj, ok := trw.w.(http.Hijacker)
	if !ok {
		return conn, nil, errors.New("Does not implement Hijacker")
	}
	return hj.Hijack()
}

func getMuxString(r *http.Request, id string) (string, error) {
	v, ok := mux.Vars(r)[id]
	if !ok {
		return "", ErrVariableNotFound
	}
	return v, nil
}

func getMuxUUID(r *http.Request, id string) (uid uuid.UUID, err error) {
	v, ok := mux.Vars(r)[id]
	if !ok {
		err = ErrVariableNotFound
	} else {
		uid, err = uuid.Parse(v)
	}
	return
}

// helper functions to get typed variables out of URLs
func getMuxInt(r *http.Request, id string, size int) (int64, error) {
	v, ok := mux.Vars(r)[id]
	if !ok {
		return 0, ErrVariableNotFound
	}
	//check that we can convert the ID to an integer
	return strconv.ParseInt(v, 10, size)
}

// helper functions to get typed variables out of URLs
func getMuxUint(r *http.Request, id string, size int) (uint64, error) {
	v, ok := mux.Vars(r)[id]
	if !ok {
		return 0, ErrVariableNotFound
	}
	//check that we can convert the ID to an integer
	return strconv.ParseUint(v, 10, size)
}

func getMuxInt64(r *http.Request, id string) (int64, error) {
	return getMuxInt(r, id, 64)
}

func getMuxUint64(r *http.Request, id string) (uint64, error) {
	return getMuxUint(r, id, 64)
}

func getMuxInt32(r *http.Request, id string) (int32, error) {
	d, err := getMuxInt(r, id, 32)
	if err != nil {
		return 0, err
	}
	return int32(d), nil
}

func getMuxUint32(r *http.Request, id string) (uint32, error) {
	d, err := getMuxUint(r, id, 32)
	if err != nil {
		return 0, err
	}
	return uint32(d), nil
}
