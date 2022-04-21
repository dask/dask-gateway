package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RoutesMsg struct {
	Routes []Route
	Id     uint64
}

type EventsMsg struct {
	Events []Event
}

type Event struct {
	Id    uint64
	Type  string
	Route Route
}

type Route struct {
	Kind      string `json:"kind"`
	SNI       string `json:"sni,omitempty"`
	Path      string `json:"path,omitempty"`
	Target    string `json:"target"`
	targetURL *url.URL
}

func (r *Route) String() string {
	return fmt.Sprintf(
		"{kind: %q, sni: %q, path: %q, target: %q}",
		r.Kind, r.SNI, r.Path, r.Target,
	)
}

var errMissingTarget = errors.New("Must specify `target`")
var errMissingSNI = errors.New("Must specify `sni`")
var errMissingPath = errors.New("Must specify `path`")
var errUnknownKind = errors.New("Unknown route kind")
var errInvalidScheme = errors.New("Scheme must be `http` or `https`")

func (r *Route) Validate(full bool) error {
	if r.Kind == "SNI" {
		if r.SNI == "" {
			return errMissingSNI
		}
	} else if r.Kind == "PATH" {
		if r.Path == "" {
			return errMissingPath
		}
	} else {
		return errUnknownKind
	}
	if full {
		if r.Target == "" {
			return errMissingTarget
		}
		if r.Kind == "SNI" {
			r.Target = strings.TrimPrefix(r.Target, "tls://")
		} else {
			targetURL, err := url.Parse(r.Target)
			if err != nil {
				return err
			}
			if targetURL.Scheme != "http" && targetURL.Scheme != "https" {
				return errInvalidScheme
			}
			r.targetURL = targetURL
		}
	}
	return nil
}

type Proxy struct {
	logger     *Logger
	sniRoutes  map[string]string
	router     *Router
	routesLock sync.RWMutex
	proxy      *httputil.ReverseProxy
}

func NewProxy(logLevel LogLevel) *Proxy {
	out := Proxy{
		logger:    NewLogger("Proxy", logLevel),
		sniRoutes: make(map[string]string),
		router:    NewRouter(),
	}
	out.proxy = &httputil.ReverseProxy{
		Director:      out.director,
		FlushInterval: 250 * time.Millisecond,
		ErrorHandler:  out.errorHandler,
	}
	return &out
}

func awaitShutdown() {
	buf := make([]byte, 10)
	for {
		_, err := os.Stdin.Read(buf)
		if err == io.EOF {
			os.Exit(0)
		}
	}
}

func (p *Proxy) run(address, tcpAddress, apiURL, apiToken, tlsCert, tlsKey string, isChildProcess bool) {
	if isChildProcess {
		go awaitShutdown()
	}

	if tcpAddress == "" {
		tcpAddress = address
	}

	tlsListener, err := net.Listen("tcp", tcpAddress)
	if err != nil {
		p.logger.Errorf("%s", err)
		os.Exit(1)
	}
	p.logger.Infof("TCP Proxy serving at %s", tcpAddress)

	var httpListener net.Listener
	var forwarder *Forwarder = nil
	if address == tcpAddress {
		forwarder = newForwarder(tlsListener)
		httpListener = forwarder
	} else {
		httpListener, err = net.Listen("tcp", address)
		if err != nil {
			p.logger.Errorf("%s", err)
			os.Exit(1)
		}
	}
	p.logger.Infof("HTTP Proxy serving at %s", address)

	go p.runHTTP(httpListener, tlsCert, tlsKey)
	go p.runTCP(tlsListener, forwarder)
	p.watchRoutes(apiURL, apiToken, 15*time.Second)
}

func (p *Proxy) clearRoutes() {
	p.logger.Infof("Resetting routing table")
	p.sniRoutes = make(map[string]string)
	p.router = NewRouter()
}

func (p *Proxy) putRoute(route *Route) {
	if err := route.Validate(true); err != nil {
		p.logger.Warnf("Invalid route %s: %s", route, err)
		return
	}

	p.logger.Infof("Adding route %s", route)

	if route.Kind == "SNI" {
		p.sniRoutes[route.SNI] = route.Target
	} else {
		p.router.Put(route.Path, route.targetURL)
	}
}

func (p *Proxy) deleteRoute(route *Route) {
	if err := route.Validate(false); err != nil {
		p.logger.Warnf("Invalid route %s: %s", route, err)
		return
	}

	p.logger.Infof("Removing route %s", route)

	if route.Kind == "SNI" {
		delete(p.sniRoutes, route.SNI)
	} else {
		p.router.Delete(route.Path)
	}
}

func (p *Proxy) UpdateFromRoutes(routes []Route) {
	p.routesLock.Lock()
	defer p.routesLock.Unlock()
	p.clearRoutes()
	for _, route := range routes {
		p.putRoute(&route)
	}
	return
}

func (p *Proxy) UpdateFromEvents(events []Event) {
	p.routesLock.Lock()
	defer p.routesLock.Unlock()
	for _, event := range events {
		if event.Type == "PUT" {
			p.putRoute(&event.Route)
		} else if event.Type == "DELETE" {
			p.deleteRoute(&event.Route)
		} else {
			p.logger.Warnf("Unknown event type %s", event.Type)
		}
	}
}

// Tracker implementation
type apiClient struct {
	client   *http.Client
	apiURL   string
	apiToken string
}

func NewApiClient(apiURL string, apiToken string) *apiClient {
	return &apiClient{
		client: &http.Client{
			Timeout: 60 * time.Second,
		},
		apiURL:   apiURL,
		apiToken: apiToken,
	}
}

func (c *apiClient) getRoutingTable() (*RoutesMsg, error) {
	req, err := http.NewRequest("GET", c.apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "token "+c.apiToken)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, errors.New(resp.Status)
	}

	var msg RoutesMsg
	err = json.NewDecoder(resp.Body).Decode(&msg)
	return &msg, err
}

type evIter struct {
	body    io.ReadCloser
	scanner *bufio.Scanner
	events  []Event
	err     error
}

func NewEvIter(body io.ReadCloser) *evIter {
	return &evIter{
		body:    body,
		scanner: bufio.NewScanner(body),
	}
}

func (it *evIter) Next() bool {
	if ok := it.scanner.Scan(); !ok {
		it.body.Close()
		return false
	}
	var msg EventsMsg
	if err := json.Unmarshal(it.scanner.Bytes(), &msg); err != nil {
		it.err = err
		it.body.Close()
		return false
	}
	it.events = msg.Events
	return true
}

func (it *evIter) Err() error {
	return it.err
}

func (it *evIter) Events() []Event {
	return it.events
}

var errTooOld = errors.New("Routes `last_id` is too old, retry")

func (c *apiClient) watchRoutingEvents(lastId uint64) (*evIter, error) {
	url := c.apiURL + "?watch=1&last_id=" + strconv.FormatUint(lastId, 10)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "token "+c.apiToken)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		if resp.StatusCode == 410 {
			return nil, errTooOld
		}
		return nil, errors.New(resp.Status)
	}
	return NewEvIter(resp.Body), nil
}

func (p *Proxy) watchRoutes(apiURL, apiToken string, maxBackoff time.Duration) {
	p.logger.Infof("Watching %s for routing table updates...", apiURL)

	initialBackoff := 500 * time.Millisecond
	backoff := initialBackoff

	doBackoff := func(msg string, err error) {
		p.logger.Warnf(msg, backoff.Seconds(), err)
		time.Sleep(backoff)
		backoff = backoff * 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	resetBackoff := func() {
		backoff = initialBackoff
	}

	client := NewApiClient(apiURL, apiToken)
	for {
		msg, err := client.getRoutingTable()
		if err != nil {
			doBackoff("Unexpected failure fetching routing table, retrying in %.1fs: %s", err)
			continue
		}
		resetBackoff()

		p.UpdateFromRoutes(msg.Routes)

		last_id := msg.Id

		for {
			it, err := client.watchRoutingEvents(last_id)
			if err == errTooOld {
				p.logger.Debugf("last_id is too old, fetching whole routing table")
				break
			} else if err != nil {
				doBackoff("Unexpected failure fetching routing events, retrying in %.1fs: %s", err)
				break
			}
			for it.Next() {
				events := it.Events()
				if len(events) > 0 {
					p.UpdateFromEvents(events)
					last_id = events[len(events)-1].Id
				}
			}
			if err := it.Err(); err != nil {
				doBackoff("Unexpected failure streaming routing events, retrying in %.1fs: %s", err)
				break
			}
		}
	}
}

// HTTP Proxy Implementation

func (p *Proxy) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	if err != context.Canceled {
		p.logger.Warnf("Web Proxy Error: %s", err)
	}
	w.WriteHeader(http.StatusBadGateway)
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	p.routesLock.RLock()
	target, path := p.router.Match(req.URL.Path)
	p.routesLock.RUnlock()

	var uri string
	if p.logger.level >= DEBUG {
		uri = req.URL.RequestURI()
	}

	if target != nil {
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
		p.logger.Infof("%s -> %s", uri, req.URL)
		p.proxy.ServeHTTP(w, req)
	} else {
		p.logger.Infof("%s not found", uri)
		http.Error(w, "Not Found", http.StatusNotFound)
	}
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

func (p *Proxy) director(req *http.Request) {
	// Request URL already modified above in ServeHTTP

	if _, ok := req.Header["User-Agent"]; !ok {
		// explicitly disable User-Agent so it's not set to default value
		req.Header.Set("User-Agent", "")
	}
}

func (p *Proxy) runHTTP(ln net.Listener, tlsCert, tlsKey string) {
	server := &http.Server{
		Handler: p,
	}
	var err error
	if tlsCert == "" {
		err = server.Serve(ln)
	} else {
		err = server.ServeTLS(ln, tlsCert, tlsKey)
	}
	if err != nil && err != http.ErrServerClosed {
		p.logger.Errorf("%s", err)
		os.Exit(1)
	}
}

// TCP proxy implementation

type Forwarder struct {
	net.Listener
	conns chan net.Conn
}

func newForwarder(ln net.Listener) *Forwarder {
	return &Forwarder{
		Listener: ln,
		conns:    make(chan net.Conn),
	}
}

func (h *Forwarder) Forward(conn net.Conn) {
	h.conns <- conn
}

func (h *Forwarder) Accept() (net.Conn, error) {
	conn := <-h.conns
	return conn, nil
}

func (p *Proxy) handleConnection(inConn *net.TCPConn, forwarder *Forwarder) {
	var err error

	sni, isTLS, pInConn, err := readSNI(inConn)
	if err != nil {
		p.logger.Debugf("Error extracting SNI: %s", err)
		inConn.Close()
		return
	}

	const sniPrefix = "daskgateway-"
	if !isTLS || !strings.HasPrefix(sni, sniPrefix) {
		if forwarder != nil {
			forwarder.Forward(pInConn)
			return
		} else {
			p.logger.Debug("Invalid connection attempt, closing")
			inConn.Close()
			return
		}
	}

	sni = sni[len(sniPrefix):]

	defer inConn.Close()

	p.routesLock.RLock()
	outAddr := p.sniRoutes[sni]
	p.routesLock.RUnlock()

	if outAddr == "" {
		p.logger.Infof("SNI %q not found", sni)
		return
	}

	p.logger.Infof("SNI %q -> %q", sni, outAddr)

	outConn, err := net.DialTimeout("tcp", outAddr, 10*time.Second)
	if err != nil {
		p.logger.Debugf("Failed to connect to destination %q: %s", outAddr, err)
		return
	}
	defer outConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	go p.proxyConnections(&wg, pInConn, outConn.(*net.TCPConn))
	go p.proxyConnections(&wg, outConn.(*net.TCPConn), pInConn)
	wg.Wait()
}

func (p *Proxy) proxyConnections(wg *sync.WaitGroup, in, out tcpConn) {
	defer wg.Done()
	if _, err := io.Copy(in, out); err != nil {
		p.logger.Debugf("Error proxying %q -> %q: %s", in.RemoteAddr(), out.RemoteAddr(), err)
	}
	in.CloseRead()
	out.CloseWrite()
}

func (p *Proxy) runTCP(ln net.Listener, forwarder *Forwarder) {
	for {
		c, err := ln.Accept()
		if err != nil {
			p.logger.Debugf("Failed to accept new connection: %s", err)
		}

		go p.handleConnection(c.(*net.TCPConn), forwarder)
	}
}

func main() {
	var (
		address        string
		tcpAddress     string
		apiURL         string
		logLevelString string
		isChildProcess bool
		tlsCert        string
		tlsKey         string
	)

	command := flag.NewFlagSet("dask-gateway-proxy", flag.ExitOnError)
	command.StringVar(&address, "address", ":8000", "HTTP proxy listening address")
	command.StringVar(&tcpAddress, "tcp-address", "", "TCP proxy listening address. If empty, `address` will be used.")
	command.StringVar(&apiURL, "api-url", "", "The URL the proxy should watch for updating the routing table")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")
	command.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")
	command.StringVar(&tlsCert, "tls-cert", "", "TLS cert to use, if any.")
	command.StringVar(&tlsKey, "tls-key", "", "TLS key to use, if any.")

	command.Parse(os.Args[1:])

	token := os.Getenv("DASK_GATEWAY_PROXY_TOKEN")
	if token == "" {
		panic("DASK_GATEWAY_PROXY_TOKEN environment variable not set")
	}

	logLevel := ParseLevel(logLevelString)

	if (tlsCert == "") != (tlsKey == "") {
		panic("Both -tls-cert and -tls-key must be set to use HTTPS")
	}

	if apiURL == "" {
		panic("-api-url must be set")
	}
	if _, err := url.Parse(apiURL); err != nil {
		panic("Invalid -api-url: " + err.Error())
	}

	proxy := NewProxy(logLevel)

	proxy.run(address, tcpAddress, apiURL, token, tlsCert, tlsKey, isChildProcess)
}
