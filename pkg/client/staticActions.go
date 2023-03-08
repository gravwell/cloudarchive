/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultDownloadCookieDuration time.Duration = 3 * time.Second
)

var (
	ErrNotAuthed = errors.New("Not Authed")
)

func (c *Client) getStaticURL(url string, obj interface{}) error {
	return c.methodStaticURL(http.MethodGet, url, obj)
}

func (c *Client) putStaticURL(url string, obj interface{}) error {
	return c.methodStaticPushURL(http.MethodPut, url, obj, nil)
}

func (c *Client) putStaticRawURL(url string, data []byte) error {
	return c.methodStaticPushRawURL(http.MethodPut, url, data, nil)
}
func (c *Client) patchStaticURL(url string, obj interface{}) error {
	return c.methodStaticPushURL(http.MethodPatch, url, obj, nil)
}

func (c *Client) postStaticURL(url string, sendObj, recvObj interface{}) error {
	return c.methodStaticPushURL(http.MethodPost, url, sendObj, recvObj)
}

func (c *Client) deleteStaticURL(url string, sendObj interface{}) error {
	return c.methodStaticPushURL(http.MethodDelete, url, sendObj, nil)
}

func (c *Client) methodStaticURL(method, url string, obj interface{}) error {
	if c.state != STATE_AUTHED {
		return ErrNoLogin
	}
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, url)
	req, err := http.NewRequest(method, uri, nil)
	if err != nil {
		return err
	}
	return c.staticRequest(req, obj, nil)
}

func (c *Client) staticRequest(req *http.Request, obj interface{}, okResponses []int) error {
	if c.state != STATE_AUTHED {
		return ErrNoLogin
	}
	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	resp, err := c.clnt.Do(req)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("Invalid response")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		c.state = STATE_LOGGED_OFF
		return ErrNotAuthed
	}
	var statOk bool
	for i := range okResponses {
		if resp.StatusCode == okResponses[i] {
			statOk = true
			break
		}
	}
	//either its in the list, or the list is empty and StatusOK is implied
	if !(statOk || (resp.StatusCode == http.StatusOK && len(okResponses) == 0)) {
		return fmt.Errorf("Bad Status %s(%d): %s", resp.Status, resp.StatusCode, getBodyErr(resp.Body))
	}

	if obj != nil {
		if err := json.NewDecoder(resp.Body).Decode(&obj); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) methodStaticPushRawURL(method, url string, data []byte, recvObj interface{}) error {
	var err error

	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, url)
	req, err := http.NewRequest(method, uri, bytes.NewBuffer(data))
	if err != nil {
		return err
	}

	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	resp, err := c.clnt.Do(req)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("Invalid response")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		c.state = STATE_LOGGED_OFF
		return ErrNotAuthed
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad Status %s(%d): %s", resp.Status, resp.StatusCode, getBodyErr(resp.Body))
	}

	if recvObj != nil {
		if err := json.NewDecoder(resp.Body).Decode(&recvObj); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) methodStaticPushURL(method, url string, sendObj, recvObj interface{}) error {
	var jsonBytes []byte
	var err error

	if sendObj != nil {
		jsonBytes, err = json.Marshal(sendObj)
		if err != nil {
			return err
		}
	}
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, url)
	req, err := http.NewRequest(method, uri, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	resp, err := c.clnt.Do(req)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("Invalid response")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		c.state = STATE_LOGGED_OFF
		return ErrNotAuthed
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad Status %s(%d): %s", resp.Status, resp.StatusCode, getBodyErr(resp.Body))
	}

	if recvObj != nil {
		if err := json.NewDecoder(resp.Body).Decode(&recvObj); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) methodRequestURL(method, url, contentType string, body io.Reader) (resp *http.Response, err error) {
	var req *http.Request
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, url)
	if req, err = http.NewRequest(method, uri, body); err != nil {
		return
	}
	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	if contentType != `` {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err = c.clnt.Do(req)
	return
}

func (c *Client) methodRequestURLWithContext(method, url, contentType string, body io.Reader, ctx context.Context) (resp *http.Response, err error) {
	var req *http.Request
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, url)
	if req, err = http.NewRequest(method, uri, body); err != nil {
		return
	}
	req = req.WithContext(ctx)
	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	if contentType != `` {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err = c.clnt.Do(req)
	return
}

// a test get without locking. For internal calls
func (c *Client) nolockTestGet(path string) error {
	uri := fmt.Sprintf("%s://%s%s", c.httpScheme, c.server, path)
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	if err != nil {
		return err
	}
	for k, v := range c.headerMap {
		req.Header.Add(k, v)
	}
	resp, err := c.clnt.Do(req)
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New("Invalid response")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		c.state = STATE_LOGGED_OFF
		return errors.New("Test GET returned StatusUnauthorized")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Bad Status %s(%d)\n", resp.Status, resp.StatusCode)
	}

	return nil
}

// getBodyErr pulls a possible error message out of the response body
// and returns it as a string.  We will yank a maximum of 256 bytes
func getBodyErr(rc io.Reader) string {
	resp := make([]byte, 256)
	n, err := rc.Read(resp)
	if (err != nil && err != io.EOF) || n <= 0 {
		return ""
	}
	return strings.TrimSpace(string(resp[0:n]))
}
