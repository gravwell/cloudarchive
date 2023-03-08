/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package webserver

import (
	"errors"
	"io"
	"net/http"
	"time"
)

type rateTimeoutReader struct {
	res http.ResponseWriter
	rdr io.ReadCloser
	tmr *time.Timer
	to  time.Duration
	err error
}

func newRateTimeoutReader(rdr io.ReadCloser, to time.Duration, res http.ResponseWriter) (rtr *rateTimeoutReader, err error) {
	rtr = &rateTimeoutReader{
		rdr: rdr,
		to:  to,
		res: res,
	}
	err = rtr.start()

	return
}

func (rtr *rateTimeoutReader) resetReader(rdr io.ReadCloser) {
	rtr.rdr = rdr
}

// the timeout function is used so that if data is not flowing through the http request.body we can
// timeout and get handler to exit.  This is done so that if a client just up and disappears and its not
// possible to get ACKs RSTs or FINs going back and forth, we still have a method to terminate the HTTP handler
// clean up the connection, and release the lock on the shard.  This is done via dirty hack.  We use hijack on
// the response writer to get a handle on the underlying net.Conn, THEN we close that conn.  This will cause
// reads to fail on the request.Body and things to exit and clean up.
func (rtr *rateTimeoutReader) timeout() {
	if rtr.rdr != nil && rtr.res != nil {
		rtr.err = errors.New("Timeout")
		if hj, ok := rtr.res.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err != nil {
				return
			} else {
				conn.Close()
			}
		}
	}
}

func (rtr *rateTimeoutReader) start() error {
	if rtr.tmr == nil {
		rtr.tmr = time.AfterFunc(rtr.to, rtr.timeout)
		return nil
	}
	return errors.New("already started")
}

func (rtr *rateTimeoutReader) Close() error {
	if rtr.tmr != nil {
		rtr.tmr.Stop()
	}
	return rtr.rdr.Close()
}

func (rtr *rateTimeoutReader) Read(b []byte) (n int, err error) {
	if rtr.err != nil {
		//short circuit out
		err = rtr.err
		return
	}
	//issue the read
	if n, err = rtr.rdr.Read(b); err == nil {
		rtr.tmr.Reset(rtr.to)
	} else if rtr.err != nil {
		//check if the internal errors should override the return of the read call
		err = rtr.err
	}
	return
}

type rateTimeoutWriter struct {
	tmr *time.Timer
	to  time.Duration
	res http.ResponseWriter
	err error
}

func newRateTimeoutWriter(res http.ResponseWriter, to time.Duration) (wtw *rateTimeoutWriter, err error) {
	wtw = &rateTimeoutWriter{
		to:  to,
		res: res,
	}
	err = wtw.start()

	return
}

// the timeout function is used so that if data is not flowing through the http response writer we can
// timeout and get handler to exit.  This is done so that if a client just up and disappears and its not
// possible to get ACKs RSTs or FINs going back and forth, we still have a method to terminate the HTTP handler
// clean up the connection, and release the lock on the shard.  This is done via dirty hack.  We use hijack on
// the response writer to get a handle on the underlying net.Conn, THEN we close that conn.  This will cause
// reads to fail on the request.Body and things to exit and clean up.
func (wtw *rateTimeoutWriter) timeout() {
	if hj, ok := wtw.res.(http.Hijacker); ok {
		if conn, _, err := hj.Hijack(); err == nil {
			conn.Close()
		}
	}
}

func (wtw *rateTimeoutWriter) start() error {
	if wtw.tmr == nil {
		wtw.tmr = time.AfterFunc(wtw.to, wtw.timeout)
		return nil
	}
	return errors.New("already started")
}

func (wtw *rateTimeoutWriter) Close() (err error) {
	if wtw.tmr != nil {
		wtw.tmr.Stop()
	}
	return nil
}

func (wtw *rateTimeoutWriter) Write(b []byte) (n int, err error) {
	if wtw.err != nil {
		//short circuit out
		err = wtw.err
		return
	} else if wtw.res == nil {
		err = errors.New("Empty connection")
		return
	}
	//issue the read
	if n, err = wtw.res.Write(b); err == nil {
		wtw.tmr.Reset(wtw.to)
	} else if wtw.err != nil {
		//check if the internal errors should override the return of the read call
		err = wtw.err
	}
	return
}
