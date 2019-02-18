package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

type HttpProxy struct {
	router *Router
	proxy  *httputil.ReverseProxy
}

func NewWebProxy() *HttpProxy {
	out := HttpProxy{
		router: NewRouter(),
	}
	out.proxy = &httputil.ReverseProxy{
		Director:      out.director,
		FlushInterval: 250 * time.Millisecond,
	}
	return &out
}

func (p *HttpProxy) AddRoute(route string, target string) {
	targetUrl, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	p.router.Put(route, targetUrl)
}

func (p *HttpProxy) RemoveRoute(route string) {
	p.router.Delete(route)
}

func (p *HttpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if addr, _ := p.router.Get(r.URL.Path); addr != nil {
		p.proxy.ServeHTTP(w, r)
		return
	}
	http.Error(w, "Not Found", http.StatusNotFound)
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
}

func (p *HttpProxy) director(req *http.Request) {
	var target *url.URL
	var path string
	target, n := p.router.Get(req.URL.Path)
	path = req.URL.Path[n:]
	targetQuery := target.RawQuery

	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
	req.Host = target.Host
	req.URL.Path = singleJoiningSlash(target.Path, path)
	if targetQuery == "" || req.URL.RawQuery == "" {
		req.URL.RawQuery = targetQuery + req.URL.RawQuery
	} else {
		req.URL.RawQuery = targetQuery + "&" + req.URL.RawQuery
	}

	if _, ok := req.Header["User-Agent"]; !ok {
		// explicitly disable User-Agent so it's not set to default value
		req.Header.Set("User-Agent", "")
	}
}

func webProxyMain(args []string) {
	var (
		address        string
		apiAddress     string
		logLevelString string
	)

	command := flag.NewFlagSet("dask-gateway-proxy web", flag.ExitOnError)

	command.StringVar(&address, "address", ":8786", "Proxy listening address")
	command.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")

	command.Parse(args)

	proxy := NewWebProxy()

	proxy.AddRoute("/foo/biz", "http://localhost:8888")
	proxy.AddRoute("/foo/baz", "http://localhost:8889")

	server := &http.Server{
		Addr:    address,
		Handler: proxy,
	}
	log.Fatal(server.ListenAndServe())
}
