/*************************************************************************
 * Copyright 2023 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"flag"
	glog "log"
	"os"
	"os/signal"

	"github.com/gravwell/cloudarchive/pkg/auth"
	"github.com/gravwell/cloudarchive/pkg/filestore"
	"github.com/gravwell/cloudarchive/pkg/ftpstore"
	"github.com/gravwell/cloudarchive/pkg/webserver"

	"github.com/gravwell/gravwell/v4/ingest/log"
)

const (
	appName string = `cloudarchive`
)

var (
	fConfig = flag.String("config-file", "", "Path to configuration file")
)

func main() {
	quitSig := make(chan os.Signal, 2)
	defer close(quitSig)
	signal.Notify(quitSig, os.Interrupt)

	flag.Parse()

	cfg, err := GetConfig(*fConfig)
	if err != nil {
		glog.Fatalf("Failed to open config %v: %v", *fConfig, err)
	}

	var lgr *log.Logger
	if cfg.Global.Log_File == `` {
		lgr = log.New(os.Stderr)
	} else {
		if lgr, err = log.NewFile(cfg.Global.Log_File); err != nil {
			glog.Fatalf("Failed to open log file %v: %v", cfg.Global.Log_File, err)
		}
	}
	lgr.SetAppname(appName)

	if err = lgr.SetLevelString(cfg.Global.Log_Level); err != nil {
		glog.Fatalf("Failed to set log level %v: %v", cfg.Global.Log_Level, err)
	}

	var handler webserver.ShardHandler
	switch cfg.Global.Backend_Type {
	case BackendTypeFile:
		handler, err = filestore.NewFilestoreHandler(cfg.Global.Storage_Directory)
		if err != nil {
			lgr.Fatalf("Failed to create a new file store handler: %v", err)
		}
	case BackendTypeFTP:
		fcfg := ftpstore.FtpStoreConfig{
			LocalStore: cfg.Global.Storage_Directory,
			FtpServer:  cfg.Global.FTP_Server,
			BaseDir:    cfg.Global.Remote_Base_Directory,
			Username:   cfg.Global.FTP_Username,
			Password:   cfg.Global.FTP_Password,
			Lgr:        lgr,
		}
		handler, err = ftpstore.NewFtpStoreHandler(fcfg)
		if err != nil {
			lgr.Fatalf("Failed to create new ftp store handler: %v", err)
		}
	}

	fileAuth, err := auth.NewAuthModule(cfg.Global.Password_File)
	if err != nil {
		lgr.Fatalf("Failed to load file based auth module: %v", err)
	}

	conf := webserver.WebserverConfig{
		ListenString: cfg.Global.Listen_Address,
		DisableTLS:   cfg.Global.Disable_TLS,
		CertFile:     cfg.Global.Cert_File,
		KeyFile:      cfg.Global.Key_File,
		Logger:       lgr,
		ShardHandler: handler,
		Auth:         fileAuth,
	}

	ws, err := webserver.NewWebserver(conf)
	if err != nil {
		glog.Fatalln("Failed to create new webserver", err)
	}

	if err = ws.Init(); err != nil {
		glog.Fatalln("Failed to init webserver", err)
	}

	if err = ws.Run(); err != nil {
		glog.Fatalln("Failed to run webserver", err)
	}

	glog.Printf("Webserver running.")

	<-quitSig

	glog.Printf("Webserver exiting.")

	if err = ws.Close(); err != nil {
		glog.Fatalln("Failed to close webserver", err)
	}
}
