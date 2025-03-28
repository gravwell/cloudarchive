/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package webserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/golang-jwt/jwt/v4"
	"github.com/gravwell/gravwell/v4/ingest/log"
)

const (
	jwtAuthHeader string = `Authorization`
)

var (
	ErrMissingJWTToken = errors.New("Missing JWT token")
)

type CustomerDetails struct {
	CustomerNumber uint64
}

type Authenticator interface {
	Authenticate(custnum, passwd string) (cid uint64, err error)
}

// AuthUser ensures the user is authenticated and allows the mux to continue
func (w *Webserver) AuthUser(res http.ResponseWriter, req *http.Request) (cust *CustomerDetails) {
	var err error
	if cust, err = w.authRequest(req); err != nil {
		cust = nil
		w.lgr.Info("AuthUser unauthorized", log.KVErr(err))
		res.WriteHeader(http.StatusUnauthorized)
	}
	return
}

func (w *Webserver) authRequest(req *http.Request) (cust *CustomerDetails, err error) {
	tok, err := w.getJWTToken(req)
	if err != nil {
		return nil, err
	}
	if cust, err = w.decodeJWTToken(tok); err != nil {
		return nil, err
	}

	return cust, nil
}

func (w *Webserver) getJWTToken(req *http.Request) (tok string, err error) {
	var n int
	if tok = req.Header.Get(jwtAuthHeader); tok == `` {
		err = ErrMissingJWTToken
		return
	}
	if n, err = fmt.Sscanf(tok, "Bearer %s", &tok); err != nil {
		return
	} else if n != 1 {
		err = ErrMissingJWTToken
		return
	}
	return
}

func (w *Webserver) decodeJWTToken(tok string) (cust *CustomerDetails, err error) {
	var token *jwt.Token
	token, err = jwt.Parse(tok, func(token *jwt.Token) (interface{}, error) {
		// Don't forget to validate the alg is what you expect:
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return w.hmacSecret, nil
	})

	if err != nil {
		return
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		cn, ok := claims["CustomerNumber"]
		if !ok {
			err = errors.New("No customer number in token claims")
		}
		custNum, ok := cn.(float64)
		if !ok {
			err = errors.New("Customer number could not be converted to a float64")
		}
		cust = &CustomerDetails{CustomerNumber: uint64(custNum)}
	}
	return
}

type loginType struct {
	User string
	Pass string
}

func (w *Webserver) loginPostPage(res http.ResponseWriter, req *http.Request) {
	var user string
	var pass string
	err := req.ParseForm()
	if err != nil {
		serverFail(res, err)
		return
	}

	if req.PostForm == nil {
		serverFail(res, errors.New("No PostForm"))
		return
	}

	//check if there is POST fields
	_, uok := req.PostForm["User"]
	_, pok := req.PostForm["Pass"]
	if !uok && !pok {
		//not using form posts, lets try JSON
		var lt loginType
		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&lt); err != nil {
			loginFail(res)
			w.lgr.Info("Invalid JSON post to login page")
			return
		}
		user = lt.User
		pass = lt.Pass
	} else {
		//using the form data
		users, ok := req.PostForm["User"]
		if !ok {
			loginFail(res)
			w.lgr.Info("Invalid Post to login page.  No \"User\" field")
			return
		}
		passes, ok := req.PostForm["Pass"]
		if !ok {
			loginFail(res)
			w.lgr.Info("Invalid Post to login page.  No \"Pass\" field")
			return
		}
		if len(users) != 1 || len(passes) != 1 {
			loginFail(res)
			w.lgr.Info("Invalid Post to login page.  Invalid \"User\" or \"Pass\" field count")
			return
		}
		user = users[0]
		pass = passes[0]
	}

	cid, err := w.authModule.Authenticate(user, pass)
	if err != nil {
		loginFail(res)
		return
	}

	// Create a new token object, specifying signing method and the claims
	// you would like it to contain.
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"CustomerNumber": cid,
	})

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString(w.hmacSecret)
	if err != nil {
		loginFail(res)
		return
	}

	w.lgr.Info("Login successful for customer", log.KV("cid", cid))
	loginSucceed(res, tokenString)
}

type LoginResponse struct {
	LoginStatus bool
	Reason      string
	JWT         string
}

func loginFail(res http.ResponseWriter) {
	res.WriteHeader(http.StatusUnprocessableEntity)
	res.Header().Set("Content-Type", "application/json")
	lr := LoginResponse{
		LoginStatus: false,
		Reason:      "Invalid username or password",
	}
	json.NewEncoder(res).Encode(lr)
}

func loginSucceed(res http.ResponseWriter, jwt string) {
	res.Header().Set("Content-Type", "application/json")
	lr := LoginResponse{
		LoginStatus: true,
		JWT:         jwt,
	}
	json.NewEncoder(res).Encode(lr)
}
