package main

import (
	"bufio"
	"bytes"
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
	tlsTimeout time.Duration
	logger     *Logger
	sniRoutes  map[string]string
	router     *Router
	proxy      *httputil.ReverseProxy
	routesLock sync.RWMutex
}

func NewProxy(tlsTimeout time.Duration, logLevel LogLevel) *Proxy {
	out := Proxy{
		tlsTimeout: tlsTimeout,
		logger:     NewLogger("Proxy", logLevel),
		sniRoutes:  make(map[string]string),
		router:     NewRouter(),
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

func (p *Proxy) run(httpAddress, tlsAddress, apiURL, apiToken, tlsCert, tlsKey string, isChildProcess bool) {
	if isChildProcess {
		go awaitShutdown()
	}
	go p.runHTTP(httpAddress, tlsCert, tlsKey)
	go p.runTLS(tlsAddress)
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

func (p *Proxy) runHTTP(address, tlsCert, tlsKey string) {
	server := &http.Server{
		Addr:    address,
		Handler: p,
	}
	var err error
	if tlsCert == "" {
		p.logger.Infof("HTTP Proxy serving at http://%s", address)
		err = server.ListenAndServe()
	} else {
		p.logger.Infof("HTTP Proxy serving at https://%s", address)
		err = server.ListenAndServeTLS(tlsCert, tlsKey)
	}
	if err != nil && err != http.ErrServerClosed {
		p.logger.Errorf("%s", err)
		os.Exit(1)
	}
}

// TLS proxy implementation

type tlsConn struct {
	inConn   *net.TCPConn
	outConn  *net.TCPConn
	sni      string
	outAddr  string
	tlsMinor int
}

type tlsAlert int8

const (
	internalError    tlsAlert = 80
	unrecognizedName          = 112
)

func (p *Proxy) sendAlert(c *tlsConn, alert tlsAlert, format string, args ...interface{}) {
	p.logger.Debugf(format, args...)

	alertMsg := []byte{21, 3, byte(c.tlsMinor), 0, 2, 2, byte(alert)}

	if err := c.inConn.SetWriteDeadline(time.Now().Add(p.tlsTimeout)); err != nil {
		p.logger.Debugf("Error while setting write deadline during abort: %s", err)
		return
	}

	if _, err := c.inConn.Write(alertMsg); err != nil {
		p.logger.Debugf("Error while sending alert: %s", err)
	}
}

func (p *Proxy) handleConnection(c *tlsConn) {
	defer c.inConn.Close()

	var err error

	if err = c.inConn.SetReadDeadline(time.Now().Add(p.tlsTimeout)); err != nil {
		p.sendAlert(c, internalError, "Setting read deadline for handshake: %s", err)
		return
	}

	var handshakeBuf bytes.Buffer
	c.sni, c.tlsMinor, err = readVerAndSNI(io.TeeReader(c.inConn, &handshakeBuf))
	if err != nil {
		p.sendAlert(c, internalError, "Extracting SNI: %s", err)
		return
	}

	if err = c.inConn.SetReadDeadline(time.Time{}); err != nil {
		p.sendAlert(c, internalError, "Clearing read deadline for handshake: %s", err)
		return
	}

	p.routesLock.RLock()
	c.outAddr = p.sniRoutes[c.sni]
	p.routesLock.RUnlock()
	if c.outAddr == "" {
		p.sendAlert(c, unrecognizedName, "SNI %q not found", c.sni)
		return
	}

	p.logger.Infof("SNI %q -> %q", c.sni, c.outAddr)

	outConn, err := net.DialTimeout("tcp", c.outAddr, 10*time.Second)
	if err != nil {
		p.sendAlert(c, internalError, "Failed to connect to destination %q: %s", c.outAddr, err)
		return
	}
	c.outConn = outConn.(*net.TCPConn)
	defer c.outConn.Close()

	if _, err = io.Copy(c.outConn, &handshakeBuf); err != nil {
		p.sendAlert(c, internalError, "Failed to replay handshake to %q: %s", c.outAddr, err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go p.proxyConnections(&wg, c.inConn, c.outConn)
	go p.proxyConnections(&wg, c.outConn, c.inConn)
	wg.Wait()
}

func (p *Proxy) proxyConnections(wg *sync.WaitGroup, in, out *net.TCPConn) {
	defer wg.Done()
	if _, err := io.Copy(in, out); err != nil {
		p.logger.Debugf("Error proxying %q -> %q: %s", in.RemoteAddr(), out.RemoteAddr(), err)
	}
	in.CloseRead()
	out.CloseWrite()
}

func (p *Proxy) runTLS(tlsAddress string) {
	p.logger.Infof("TLS Proxy serving at %s", tlsAddress)
	l, err := net.Listen("tcp", tlsAddress)
	if err != nil {
		p.logger.Errorf("%s", err)
		os.Exit(1)
	}
	for {
		c, err := l.Accept()
		if err != nil {
			p.logger.Debugf("Failed to accept new connection: %s", err)
		}

		conn := &tlsConn{inConn: c.(*net.TCPConn)}
		go p.handleConnection(conn)
	}
}

func main() {
	var (
		httpAddress    string
		tlsAddress     string
		apiURL         string
		logLevelString string
		isChildProcess bool
		tlsCert        string
		tlsKey         string
		tlsTimeout     time.Duration
	)

	command := flag.NewFlagSet("dask-gateway-proxy", flag.ExitOnError)
	command.StringVar(&httpAddress, "address", ":8000", "HTTP proxy listening address")
	command.StringVar(&tlsAddress, "tls-address", ":8786", "TLS proxy listening address")
	command.StringVar(&apiURL, "api-url", "", "The URL the proxy should watch for updating the routing table")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")
	command.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")
	command.StringVar(&tlsCert, "tls-cert", "", "TLS cert to use, if any.")
	command.StringVar(&tlsKey, "tls-key", "", "TLS key to use, if any.")
	command.DurationVar(&tlsTimeout, "tls-timeout", 3*time.Second, "Timeout for TLS Handshake initialization")

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

	proxy := NewProxy(tlsTimeout, logLevel)

	proxy.run(httpAddress, tlsAddress, apiURL, token, tlsCert, tlsKey, isChildProcess)
}
