package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
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

type tlsAlert int8

const (
	internalError    tlsAlert = 80
	unrecognizedName          = 112
)

func NewSchedulerProxy(address string, apiAddress string, token string,
	timeout time.Duration, logLevel LogLevel) *Proxy {
	return &Proxy{
		address:    address,
		apiAddress: apiAddress,
		timeout:    timeout,
		logger:     NewLogger("SchedulerProxy", logLevel),
		token:      token,
		routes:     make(map[string]string),
	}
}

func (p *Proxy) routesHandler(w http.ResponseWriter, r *http.Request) {
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
			target, err := url.Parse(msg.Target)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
			}
			p.routesLock.Lock()
			p.routes[route] = target.Host
			p.routesLock.Unlock()
			p.logger.Debugf("Added route %s -> %s", route, target.Host)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodDelete:
			p.routesLock.Lock()
			delete(p.routes, route)
			p.routesLock.Unlock()
			p.logger.Debugf("Removed route %s", route)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func (p *Proxy) Run(isChildProcess bool) {
	p.logger.Infof("API serving at %s", p.apiAddress)
	go serveAPI(p.routesHandler, p.apiAddress, p.token, p.logger)

	p.logger.Infof("Proxy serving at %s", p.address)
	l, err := net.Listen("tcp", p.address)
	if err != nil {
		p.logger.Errorf("%s", err)
		os.Exit(1)
	}

	if isChildProcess {
		go awaitShutdown()
	}
	for {
		c, err := l.Accept()
		if err != nil {
			p.logger.Warnf("Failed to accept new connection: %s", err)
		}

		conn := &Conn{inConn: c.(*net.TCPConn)}
		go p.proxy(conn)
	}
}

func (p *Proxy) sendAlert(c *Conn, alert tlsAlert, format string, args ...interface{}) {
	if alert == unrecognizedName {
		p.logger.Debugf(format, args...)
	} else {
		p.logger.Warnf(format, args...)
	}

	alertMsg := []byte{21, 3, byte(c.tlsMinor), 0, 2, 2, byte(alert)}

	if err := c.inConn.SetWriteDeadline(time.Now().Add(p.timeout)); err != nil {
		p.logger.Warnf("Error while setting write deadline during abort: %s", err)
		return
	}

	if _, err := c.inConn.Write(alertMsg); err != nil {
		p.logger.Warnf("Error while sending alert: %s", err)
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

	if err = c.inConn.SetReadDeadline(time.Time{}); err != nil {
		p.sendAlert(c, internalError, "Clearing read deadline for handshake: %s", err)
		return
	}

	c.outAddr = p.routes[c.sni]
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

func schedulerProxyMain(args []string) {
	var (
		address        string
		apiAddress     string
		timeout        time.Duration
		logLevelString string
		isChildProcess bool
	)

	command := flag.NewFlagSet("dask-gateway-proxy scheduler", flag.ExitOnError)

	command.StringVar(&address, "address", ":8786", "Proxy listening address")
	command.StringVar(&apiAddress, "api-address", ":8787", "API listening address")
	command.DurationVar(&timeout, "timeout", 3*time.Second, "Timeout for TLS Handshake initialization")
	command.StringVar(&logLevelString, "log-level", "INFO",
		"The log level. One of {DEBUG, INFO, WARN, ERROR}")
	command.BoolVar(&isChildProcess, "is-child-process", false,
		"If set, will exit when stdin EOFs. Useful when running as a child process.")

	command.Parse(args)

	token := os.Getenv("DASK_GATEWAY_PROXY_TOKEN")
	if token == "" {
		panic("DASK_GATEWAY_PROXY_TOKEN environment variable not set")
	}
	logLevel := ParseLevel(logLevelString)
	p := NewSchedulerProxy(address, apiAddress, token, timeout, logLevel)
	p.Run(isChildProcess)
}
