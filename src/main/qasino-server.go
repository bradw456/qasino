// Copyright (C) 2016 MediaMath, Inc. <http://www.mediamath.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	netpprof "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"qasino/server"
	"qasino/util"

	auth "github.com/abbot/go-http-auth"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var htpasswd *string = pflag.StringP("htpasswd", "P", "", "Htpasswd file to enforce basic auth")
var certfile *string = pflag.StringP("certfile", "C", "server.crt", "Cert file for TLS")
var keyfile *string = pflag.StringP("keyfile", "K", "server.key", "Key file for TLS")
var files_path *string = pflag.StringP("files-path", "f", "etc", "Path to location of server files")
var config_path *string = pflag.StringP("config-path", "c", "etc", "Path to location of config.yaml")
var identity *string = pflag.StringP("identity", "i", "server_identity", "Use identity")
var db_dir *string = pflag.StringP("db-dir", "d", "/ramdisk/qasino/dbs", "Path to sqlite database location")
var generation_duration_s *int = pflag.IntP("generation-duration", "g", 30, "The length of a collection interval (generation) in seconds")
var views_file *string = pflag.StringP("views-file", "v", "views.conf", "A file containing a list of views to create.")

func MyBasicAuth(a *auth.BasicAuth) gin.HandlerFunc {
	return func(c *gin.Context) {
		if username := a.CheckAuth(c.Request); username == "" {
			// Credentials don't match, we return 401 and abort handlers chain.
			c.Header("WWW-Authenticate", `Basic realm="`+a.Realm+`"`)
			c.String(401, "401 Unauthorized\n")
			c.AbortWithStatus(401)
		}
	}
}

var skip_high_traffic_paths = []string{"/request"}

func OurGinRouter() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.LoggerWithWriter(gin.DefaultWriter, skip_high_traffic_paths...), gin.Recovery())
	return engine
}

func main() {

	// For glog, log to stderr/out
	flag.Set("logtostderr", "true")
	flag.Set("v", "1") // start with basic info logging.

	// Hack for glog to get all its standard flags into pflags.
	// http://grokbase.com/t/gg/golang-nuts/1518cxqaac/go-nuts-use-glog-with-alternate-flags-library
	util.AddAllFlagsToPFlagSet(pflag.CommandLine)

	// And reset standard flags...
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flag.CommandLine.Parse(nil) // needed by glog to set 'parsed'

	pflag.Parse()

	viper.SetConfigType("yaml")
	viper.SetConfigName("config")
	viper.AddConfigPath(*config_path)
	viper.AddConfigPath(".")
	viper.AddConfigPath("$HOME/.qasino")
	viper.AddConfigPath("/opt/qasino/etc/")
	viper.AddConfigPath("/etc/qasino/")
	err := viper.ReadInConfig()
	if err != nil {
		glog.Errorf("Failed to find or read a valid config file (config.yaml) in /etc/qasino, ~/.qasino or .: %s\n", err)
		os.Exit(1)
	}

	viper.BindPFlag("identity", pflag.Lookup("identity"))
	viper.BindPFlag("htpasswd", pflag.Lookup("htpasswd"))
	viper.BindPFlag("certfile", pflag.Lookup("certfile"))
	viper.BindPFlag("keyfile", pflag.Lookup("keyfile"))
	viper.BindPFlag("files-path", pflag.Lookup("files-path"))

	viper.WatchConfig()
	viper.OnConfigChange(func(e fsnotify.Event) {
		glog.V(1).Infoln("Config file changed: ", e.Name)
	})

	glog.Infoln("Qasino server starting")

	redir_port := util.ConfGetIntDefault("redir_port", 80)
	https_port := util.ConfGetIntDefault("https_port", 443)
	http_rpc_port := util.ConfGetIntDefault("http_rpc_port", 15597)
	zmq_rpc_port := util.ConfGetIntDefault("zmq_rpc_port", 15598)
	zmq_pubsub_port := util.ConfGetIntDefault("zmq_pubsub_port", 15596)
	line_port := util.ConfGetIntDefault("line_port", 15000)

	http_rpc_router := OurGinRouter()
	https_router := OurGinRouter()

	signal_channel := server.NewSignalChannel()

	err = signal_channel.Open(zmq_pubsub_port)
	if err != nil {
		glog.Errorf("Failed to open ZeroMQ pub/sub listener for generation signals: %s\n", err)
		return
	}

	data_manager := server.NewDataManager(*generation_duration_s, *identity, signal_channel)

	err = data_manager.Init()
	if err != nil {
		glog.Errorf("Failed to Init data manager: %s\n", err)
		return
	}

	zmq_receiver, err := server.NewZmqReceiver(zmq_rpc_port)

	if err != nil {
		glog.Errorf("Failed to initialize the ZeroMQ Receiver: %s\n", err)
		return
	}

	signal_chan := make(chan os.Signal, 0)
	signal.Notify(signal_chan, os.Interrupt, os.Kill)

	go func() {
		select {
		case sig := <-signal_chan:
			glog.Infof("Received %s signal, exiting\n", sig)
			data_manager.Shutdown()
			os.Exit(0)
		}
	}()

	http_rpc_router.POST("/request", func(c *gin.Context) {
		server.LegacyHandlePOST_request(c, data_manager)
	})

	http_rpc_router.GET("/request", func(c *gin.Context) {
		server.LegacyHandleGET_request(c, data_manager)
	})

	// https_router acts as both RPC and UI

	tmpl, err := template.New("htdocs").ParseGlob(fmt.Sprintf("%s/files/templates/*", *files_path))
	if err != nil {
		panic(err)
	}

	https_router.SetHTMLTemplate(tmpl)

	https_router.POST("/request", func(c *gin.Context) {
		server.LegacyHandlePOST_request(c, data_manager)
	})

	https_router.GET("/", func(c *gin.Context) {
		server.UIHandleGET_tables(c, data_manager)
	})

	https_router.GET("/tables", func(c *gin.Context) {
		server.UIHandleGET_tables(c, data_manager)
	})

	https_router.GET("/desc", func(c *gin.Context) {
		server.UIHandleGET_desc(c, data_manager)
	})

	https_router.GET("/query", func(c *gin.Context) {
		server.UIHandleGET_query(c, data_manager)
	})

	staticgrp := https_router.Group("/static")
	{
		staticgrp.StaticFS("/js", http.Dir(fmt.Sprintf("%s/files/htdocs/js", *files_path)))
		staticgrp.StaticFS("/css", http.Dir(fmt.Sprintf("%s/files/htdocs/css", *files_path)))
		staticgrp.StaticFS("/images", http.Dir(fmt.Sprintf("%s/files/htdocs/images", *files_path)))
	}

	if *htpasswd != "" {
		htpasswd := auth.HtpasswdFileProvider(*htpasswd)

		realm := "Authorization Required"
		authenticator := auth.NewBasicAuthenticator(realm, htpasswd)

		https_router.Use(MyBasicAuth(authenticator))
	}

	debuggrp := https_router.Group("/debug")
	{
		debuggrp.GET("/pprof", func(c *gin.Context) {
			ProfileIndex(c.Writer, c.Request)
		})
		debuggrp.GET("/pprof/:profile", func(c *gin.Context) {
			param := c.Param("profile")
			if param == "profile" {
				netpprof.Profile(c.Writer, c.Request)
			} else if param == "symbol" {
				netpprof.Symbol(c.Writer, c.Request)
			} else if param == "trace" {
				netpprof.Trace(c.Writer, c.Request)
			} else {
				NamedProfile(c.Writer, c.Request, param)
			}
		})
	}

	// Redirect port to https_port?
	if redir_port != -1 {

		redir := gin.Default()

		redir.NoRoute(func(c *gin.Context) {

			var new_host string
			// Check for port (it will only not be there for port 80)
			if !strings.Contains(c.Request.Host, fmt.Sprintf(":%d", redir_port)) {
				new_host = fmt.Sprintf("%s:%d", c.Request.Host, https_port)
			} else {
				new_host = strings.Replace(c.Request.Host, fmt.Sprintf(":%d", redir_port), fmt.Sprintf(":%d", https_port), 1)
			}

			c.Redirect(http.StatusMovedPermanently, fmt.Sprintf("https://%s%s", new_host, c.Request.URL))
		})

		go redir.Run(fmt.Sprintf(":%d", redir_port))
	}

	go zmq_receiver.Run(data_manager)

	go http_rpc_router.Run(fmt.Sprintf(":%d", http_rpc_port))

	go server.LineReceiverAccept(line_port, data_manager)

	https_router.RunTLS(fmt.Sprintf(":%d", https_port), *certfile, *keyfile)
}

// Index responds to a request for "/debug/pprof/" with an HTML page
// listing the available profiles.
func ProfileIndex(w http.ResponseWriter, r *http.Request) {
	profiles := pprof.Profiles()
	if err := indexTmpl.Execute(w, profiles); err != nil {
		log.Print(err)
	}
}

var indexTmpl = template.Must(template.New("index").Parse(`<html>
<head>
<title>Pprof profiles</title>
</head>
<body>
Available pprof profiles:<br>

<ul>
{{range .}}
  <li><a href="/qasino/debug/pprof/{{.Name}}?debug=1">{{.Name}}</a> ({{.Count}})</li>
{{end}}
  <li><a href="/qasino/debug/pprof/goroutine?debug=2">goroutine (full stack)</a></li>
</ul>
</body>
</html>
`))

func NamedProfile(w http.ResponseWriter, r *http.Request, name string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	debug, _ := strconv.Atoi(r.FormValue("debug"))
	p := pprof.Lookup(string(name))
	if p == nil {
		w.WriteHeader(404)
		fmt.Fprintf(w, "Unknown profile: %s\n", name)
		return
	}
	gc, _ := strconv.Atoi(r.FormValue("gc"))
	if name == "heap" && gc > 0 {
		runtime.GC()
	}
	p.WriteTo(w, debug)
	return
}
