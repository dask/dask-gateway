package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	address        string
	apiAddress     string
	timeout        time.Duration
	logLevelString string
	isChildProcess bool
)

type Proxy struct {
	address       string
	apiAddress    string
	configAddress string
	timeout       time.Duration
	logger        *Logger
	token         string
	routes        map[string]string
	routesLock    sync.RWMutex
}

type Conn struct {
	inConn   *net.TCPConn
	outConn  *net.TCPConn
	sni      string
	outAddr  string
	tlsMinor int
}

type RouteMsg struct {
	Target string `json:"target"`
}

type tlsAlert int8

const (
	internalError    tlsAlert = 80
	unrecognizedName          = 112
)

func NewProxy(address string, apiAddress string, token string, timeout time.Duration, logLevel LogLevel) *Proxy {
	return &Proxy{
		address:    address,
		apiAddress: apiAddress,
		timeout:    timeout,
		logger:     NewLogger(logLevel),
		token:      token,
		routes:     make(map[string]string),
	}
}

func (p *Proxy) isAuthorized(r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != "token" || parts[1] != p.token {
		return false
	}
	return true
}

func (p *Proxy) routesHandler(w http.ResponseWriter, r *http.Request) {
	if !p.isAuthorized(r) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	route := r.URL.Path[len("/api/routes/"):]
	if route == "" {
		switch r.Method {
		case http.MethodGet:
			p.routesLock.RLock()
			js, err := json.Marshal(p.routes)
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
				http.Error(w, "Bad Request", http.StatusBadRequest)
			}
			p.routesLock.Lock()
			p.routes[route] = msg.Target
			p.routesLock.Unlock()
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			p.routesLock.Lock()
			delete(p.routes, route)
			p.routesLock.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (p *Proxy) serveAPI() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/routes/", p.routesHandler)

	server := &http.Server{
		Addr:    p.apiAddress,
		Handler: mux,
	}
	server.ListenAndServe()
}

func (p *Proxy) awaitShutdown() {
	buf := make([]byte, 10)
	for {
		_, err := os.Stdin.Read(buf)
		if err == io.EOF {
			os.Exit(0)
		}
	}
}

func (p *Proxy) Run(isChildProcess bool) {
	l, err := net.Listen("tcp", p.address)
	if err != nil {
		p.logger.Errorf("Failed to connect: %s", err)
		return
	}
	p.logger.Infof("Proxy serving at %s", p.address)
	p.logger.Infof("API serving at %s", p.apiAddress)
	go p.serveAPI()
	if isChildProcess {
		go p.awaitShutdown()
	}
	for {
		c, err := l.Accept()
		if err != nil {
			p.logger.Errorf("Failed to accept new connection: %s", err)
		}

		conn := &Conn{inConn: c.(*net.TCPConn)}
		go p.proxy(conn)
	}
}

func (p *Proxy) sendAlert(c *Conn, alert tlsAlert, format string, args ...interface{}) {
	p.logger.Errorf(format, args...)

	alertMsg := []byte{21, 3, byte(c.tlsMinor), 0, 2, 2, byte(alert)}

	if err := c.inConn.SetWriteDeadline(time.Now().Add(p.timeout)); err != nil {
		p.logger.Errorf("Error while setting write deadline during abort: %s", err)
		return
	}

	if _, err := c.inConn.Write(alertMsg); err != nil {
		p.logger.Errorf("Error while sending alert: %s", err)
	}
}

func (p *Proxy) proxy(c *Conn) {
	defer c.inConn.Close()

	var err error

	if err = c.inConn.SetReadDeadline(time.Now().Add(p.timeout)); err != nil {
		p.sendAlert(c, internalError, "Setting read deadline for handshake: %s", err)
		return
	}

	var handshakeBuf bytes.Buffer
	c.sni, c.tlsMinor, err = readVerAndSNI(io.TeeReader(c.inConn, &handshakeBuf))
	if err != nil {
		p.sendAlert(c, internalError, "Extracting SNI: %s", err)
		return
	}
	p.logger.Debugf("SNI extracted: %s", c.sni)

	if err = c.inConn.SetReadDeadline(time.Time{}); err != nil {
		p.sendAlert(c, internalError, "Clearing read deadline for handshake: %s", err)
		return
	}

	c.outAddr = p.routes[c.sni]
	if c.outAddr == "" {
		p.sendAlert(c, unrecognizedName, "No destination found for %q", c.sni)
		return
	}

	p.logger.Infof("Routing connection to %q", c.outAddr)

	outConn, err := net.DialTimeout("tcp", c.outAddr, 10*time.Second)
	if err != nil {
		p.sendAlert(c, internalError, "Failed to connect to destination %q: %s", c.outAddr, err)
		return
	}
	c.outConn = outConn.(*net.TCPConn)
	defer c.outConn.Close()

	if _, err = io.Copy(c.outConn, &handshakeBuf); err != nil {
		p.sendAlert(c, internalError, "Failed to replay handshake to %q: %s", c.outConn, err)
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
		p.logger.Errorf("Error proxying %s -> %s: %s", in.RemoteAddr(), out.RemoteAddr(), err)
	}
	in.CloseRead()
	out.CloseWrite()
}

func init() {
	flag.StringVar(&address, "address", ":8786", "Proxy listening address")
	flag.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
	flag.DurationVar(&timeout, "timeout", 3*time.Second, "Timeout for TLS Handshake initialization")
	flag.StringVar(&logLevelString, "log-level", "INFO", "The log level. One of {DEBUG, INFO, WARN, ERROR}")
	flag.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")
}

func main() {
	flag.Parse()
	token := os.Getenv("CONFIG_TLS_PROXY_TOKEN")
	if token == "" {
		panic("CONFIG_TLS_PROXY_TOKEN environment variable not set")
	}
	logLevel := ParseLevel(logLevelString)
	p := NewProxy(address, apiAddress, token, timeout, logLevel)
	p.Run(isChildProcess)
}
