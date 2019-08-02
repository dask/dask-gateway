package main

import (
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type HttpProxy struct {
	router     *Router
	proxy      *httputil.ReverseProxy
	logger     *Logger
	token      string
	routesLock sync.RWMutex
}

func NewWebProxy(logLevel LogLevel) *HttpProxy {
	out := HttpProxy{
		router: NewRouter(),
		logger: NewLogger("WebProxy", logLevel),
	}
	out.proxy = &httputil.ReverseProxy{
		Director:      out.director,
		FlushInterval: 250 * time.Millisecond,
		ErrorHandler:  out.errorHandler,
	}
	return &out
}

func (p *HttpProxy) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	if err != context.Canceled {
		p.logger.Warnf("Web Proxy Error: %s", err)
	}
	w.WriteHeader(http.StatusBadGateway)
}

func (p *HttpProxy) routesHandler(w http.ResponseWriter, r *http.Request) {
	route := r.URL.Path[len("/api/routes"):]
	if route == "/" {
		switch r.Method {
		case http.MethodGet:
			p.routesLock.RLock()
			js, err := json.Marshal(p.router)
			p.routesLock.RUnlock()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(js)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	} else {
		switch r.Method {
		case http.MethodPut:
			if r.Body == nil {
				http.Error(w, "Bad Request", http.StatusBadRequest)
				return
			}
			decoder := json.NewDecoder(r.Body)
			var msg RouteMsg
			err := decoder.Decode(&msg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			p.routesLock.Lock()
			target, err := url.Parse(msg.Target)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			p.router.Put(route, target)
			p.routesLock.Unlock()
			p.logger.Debugf("Added route %s -> %s", route, target)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			p.routesLock.Lock()
			p.router.Delete(route)
			p.routesLock.Unlock()
			p.logger.Debugf("Removed route %s", route)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (p *HttpProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.router.HasMatch(r.URL.Path) {
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
	target, path := p.router.Match(req.URL.Path)
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
		isChildProcess bool
		tlsCert        string
		tlsKey         string
	)

	command := flag.NewFlagSet("dask-gateway-proxy web", flag.ExitOnError)

	command.StringVar(&address, "address", ":8786", "Proxy listening address")
	command.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")
	command.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")
	command.StringVar(&tlsCert, "tls-cert", "", "TLS cert to use, if any.")
	command.StringVar(&tlsKey, "tls-key", "", "TLS key to use, if any.")

	command.Parse(args)

	token := os.Getenv("DASK_GATEWAY_PROXY_TOKEN")
	if token == "" {
		panic("DASK_GATEWAY_PROXY_TOKEN environment variable not set")
	}
	logLevel := ParseLevel(logLevelString)

	if (tlsCert == "") != (tlsKey == "") {
		panic("Both -tls-cert and -tls-key must be set to use HTTPS")
	}

	proxy := NewWebProxy(logLevel)

	server := &http.Server{
		Addr:    address,
		Handler: proxy,
	}
	if isChildProcess {
		go awaitShutdown()
	}
	proxy.logger.Infof("API serving at %s", apiAddress)
	go serveAPI(proxy.routesHandler, apiAddress, token, proxy.logger)

	var err error
	if tlsCert == "" {
		proxy.logger.Infof("Proxy serving at http://%s", address)
		err = server.ListenAndServe()
	} else {
		proxy.logger.Infof("Proxy serving at https://%s", address)
		err = server.ListenAndServeTLS(tlsCert, tlsKey)
	}
	if err != nil && err != http.ErrServerClosed {
		proxy.logger.Errorf("%s", err)
		os.Exit(1)
	}
}
