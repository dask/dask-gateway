package main

import (
	"bytes"
	"flag"
	"io"
	"net"
	"sync"
	"time"
)

var (
	address        string
	timeout        time.Duration
	logLevelString string
)

type Proxy struct {
	address string
	timeout time.Duration
	logger  *Logger
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

func NewProxy(address string, timeout time.Duration, logLevel LogLevel) *Proxy {
	logger := NewLogger(logLevel)
	return &Proxy{address: address, timeout: timeout, logger: logger}
}

func (p *Proxy) Run() {
	l, err := net.Listen("tcp", p.address)
	if err != nil {
		p.logger.Errorf("Failed to connect: %s", err)
		return
	}
	p.logger.Infof("Serving at %s", p.address)
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

func (p *Proxy) match(sni string) string {
	return "172.217.11.46:443"
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

	c.outAddr = p.match(c.sni)
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
	flag.StringVar(&address, "address", ":443", "Listening address")
	flag.DurationVar(&timeout, "timeout", 3*time.Second, "Timeout for TLS Handshake initialization")
	flag.StringVar(&logLevelString, "log-level", "INFO", "The log level. One of {DEBUG, INFO, WARN, ERROR}")
}

func main() {
	flag.Parse()
	logLevel := ParseLevel(logLevelString)
	p := NewProxy(address, timeout, logLevel)
	p.Run()
}
