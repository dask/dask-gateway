package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/rpc"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Route struct {
	SNI       string `json:"sni,omitempty"`
	Path      string `json:"path,omitempty"`
	Target    string `json:"target"`
	targetURL *url.URL
	target    string
}

var errBadTarget = errors.New("Failed to parse `target` in route")
var errMissingTarget = errors.New("Must specify `target`")
var errMissingRoute = errors.New("Must specify either `sni` or `path`")
var errExtraRoute = errors.New("Cannot specify both `sni` and `path`")

func (r *Route) Validate(full bool) error {
	if r.SNI != "" {
		if r.Path != "" {
			return errExtraRoute
		}

	} else if r.Path == "" {
		return errMissingRoute
	}
	if full {
		if r.Target == "" {
			return errMissingTarget
		}
		if r.SNI != "" {
			r.target = strings.TrimPrefix(r.Target, "tls://")
		} else {
			targetURL, err := url.Parse(r.Target)
			if err != nil {
				return errBadTarget
			}
			r.targetURL = targetURL
		}
	}
	return nil
}

type Request struct {
	Routes []*Route `json:"routes"`
	Reset  bool     `json:"reset"`
}

func (req *Request) Validate(full bool) (err error) {
	for _, r := range req.Routes {
		if err = r.Validate(full); err != nil {
			return
		}
	}
	return
}

type EmptyMsg struct {
}

type GetResp struct {
	Routes []*Route `json:"routes"`
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

func (p *Proxy) run(httpAddress, tlsAddress, apiAddress, token, tlsCert, tlsKey string, isChildProcess bool) {
	rpc.RegisterName("Proxy", &ProxyHandlers{p})
	if isChildProcess {
		go awaitShutdown()
	}
	go p.runHTTP(httpAddress, tlsCert, tlsKey)
	go p.runTLS(tlsAddress)
	listener, _ := net.Listen("tcp", apiAddress)
	defer listener.Close()
	p.logger.Infof("API serving at tcp://%s", apiAddress)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go ServeRpcConn(token, p.logger, conn)
	}
}

type ProxyHandlers struct {
	proxy *Proxy
}

func clearRoutes(proxy *Proxy) {
	proxy.sniRoutes = make(map[string]string)
	proxy.router = NewRouter()
}

func (h *ProxyHandlers) Update(req *Request, res *EmptyMsg) (err error) {
	if err = req.Validate(true); err != nil {
		return
	}
	h.proxy.routesLock.Lock()
	defer h.proxy.routesLock.Unlock()
	if req.Reset {
		clearRoutes(h.proxy)
	}
	for _, route := range req.Routes {
		if route.SNI != "" {
			h.proxy.sniRoutes[route.SNI] = route.target
		} else {
			h.proxy.router.Put(route.Path, route.targetURL)
		}
	}
	return
}

func (h *ProxyHandlers) Delete(req *Request, res *EmptyMsg) (err error) {
	if err = req.Validate(false); err != nil {
		return
	}
	h.proxy.routesLock.Lock()
	defer h.proxy.routesLock.Unlock()
	if req.Reset {
		clearRoutes(h.proxy)
	} else {
		for _, route := range req.Routes {
			if route.SNI != "" {
				delete(h.proxy.sniRoutes, route.SNI)
			} else {
				h.proxy.router.Delete(route.Path)
			}
		}
	}
	return
}

func (h *ProxyHandlers) Get(req *EmptyMsg, res *GetResp) (err error) {
	h.proxy.routesLock.RLock()
	defer h.proxy.routesLock.RUnlock()

	routes := make([]*Route, 0)
	for sni, target := range h.proxy.sniRoutes {
		routes = append(routes, &Route{SNI: sni, Target: target})
	}
	h.proxy.router.traverse(
		"",
		func(path string, url *url.URL) {
			routes = append(routes, &Route{Path: path, Target: url.String()})
		},
	)
	res.Routes = routes
	return
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
		p.proxy.ServeHTTP(w, req)
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
	if alert == unrecognizedName {
		p.logger.Debugf(format, args...)
	} else {
		p.logger.Warnf(format, args...)
	}

	alertMsg := []byte{21, 3, byte(c.tlsMinor), 0, 2, 2, byte(alert)}

	if err := c.inConn.SetWriteDeadline(time.Now().Add(p.tlsTimeout)); err != nil {
		p.logger.Warnf("Error while setting write deadline during abort: %s", err)
		return
	}

	if _, err := c.inConn.Write(alertMsg); err != nil {
		p.logger.Warnf("Error while sending alert: %s", err)
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
		p.sendAlert(c, unrecognizedName, "No destination found for %q", c.sni)
		return
	}

	p.logger.Debugf("Routing connection to %q", c.outAddr)

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
		p.logger.Warnf("Error proxying %q -> %q: %s", in.RemoteAddr(), out.RemoteAddr(), err)
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
			p.logger.Warnf("Failed to accept new connection: %s", err)
		}

		conn := &tlsConn{inConn: c.(*net.TCPConn)}
		go p.handleConnection(conn)
	}
}

func main() {
	var (
		httpAddress    string
		tlsAddress     string
		apiAddress     string
		logLevelString string
		isChildProcess bool
		tlsCert        string
		tlsKey         string
		tlsTimeout     time.Duration
	)

	command := flag.NewFlagSet("dask-gateway-proxy web", flag.ExitOnError)
	command.StringVar(&httpAddress, "address", ":8000", "HTTP proxy listening address")
	command.StringVar(&tlsAddress, "tls-address", ":8786", "TLS proxy listening address")
	command.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
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

	proxy := NewProxy(tlsTimeout, logLevel)

	proxy.run(httpAddress, tlsAddress, apiAddress, token, tlsCert, tlsKey, isChildProcess)
}
