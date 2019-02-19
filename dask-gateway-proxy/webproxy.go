package main

import (
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
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			p.routesLock.Lock()
			p.router.Delete(route)
			p.routesLock.Unlock()
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
	)

	command := flag.NewFlagSet("dask-gateway-proxy web", flag.ExitOnError)

	command.StringVar(&address, "address", ":8786", "Proxy listening address")
	command.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")
	command.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")

	command.Parse(args)

	token := os.Getenv("DASK_GATEWAY_PROXY_TOKEN")
	if token == "" {
		panic("DASK_GATEWAY_PROXY_TOKEN environment variable not set")
	}

	proxy := NewWebProxy()

	server := &http.Server{
		Addr:    address,
		Handler: proxy,
	}
	if isChildProcess {
		go awaitShutdown()
	}
	go serveAPI(proxy.routesHandler, apiAddress, token)

	server.ListenAndServe()
}
