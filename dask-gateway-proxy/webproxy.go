package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const (
	routeLength       = 32
	routePrefix       = "/dashboard/"
	routePrefixLength = len(routePrefix)
	hubPrefix         = "/hub/"
	hubPrefixLength   = len(hubPrefix)
)

type HttpProxy struct {
	routes map[string]*url.URL
	hub    *url.URL
	proxy  *httputil.ReverseProxy
}

func NewWebProxy() *HttpProxy {
	out := HttpProxy{
		routes: make(map[string]*url.URL),
	}
	out.proxy = &httputil.ReverseProxy{
		Director:      out.director,
		FlushInterval: 250 * time.Millisecond,
	}
	return &out
}

func (p *HttpProxy) SetHubAddress(address string) {
	hub, err := url.Parse(address)
	if err != nil {
		panic(err)
	}
	p.hub = hub
}

func (p *HttpProxy) ClearHubAddress() {
	p.hub = nil
}

func (p *HttpProxy) AddRoute(route string, target string) {
	if len(route) != routeLength {
		panic(fmt.Sprintf("len(route) must be %d, got %d",
			routeLength, len(route)))
	}
	targetUrl, err := url.Parse(target)
	if err != nil {
		panic(err)
	}
	p.routes[route] = targetUrl
}

func (p *HttpProxy) RemoveRoute(route string) {
	delete(p.routes, route)
}

func (p *HttpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/hub/") {
		p.proxy.ServeHTTP(w, r)
		return
	}

	if strings.HasPrefix(r.URL.Path, routePrefix) && len(r.URL.Path) > (routePrefixLength+routeLength) {
		key := r.URL.Path[routePrefixLength : routePrefixLength+routeLength]
		log.Printf("Key = %s", key)
		if _, ok := p.routes[key]; ok {
			p.proxy.ServeHTTP(w, r)
			return
		}
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
	if strings.HasPrefix(req.URL.Path, hubPrefix) {
		target = p.hub
		path = req.URL.Path[hubPrefixLength:]
	} else {
		key := req.URL.Path[routePrefixLength : routePrefixLength+routeLength]
		target = p.routes[key]
		path = req.URL.Path[routePrefixLength+routeLength:]
	}
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

	server := &http.Server{
		Addr:    address,
		Handler: proxy,
	}
	log.Fatal(server.ListenAndServe())
}
