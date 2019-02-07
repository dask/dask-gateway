package main

import (
	"flag"
	"time"
)

var (
	port           int
	timeout        time.Duration
	logLevelString string
)

type Proxy struct {
	port    int
	timeout time.Duration
	logger  *Logger
}

func NewProxy(port int, timeout time.Duration, logLevel LogLevel) *Proxy {
	logger := NewLogger(logLevel)
	return &Proxy{port: port, timeout: timeout, logger: logger}
}

func (p *Proxy) Run() {
	p.logger.Infof("Running on port %d, with timeout %s", p.port, p.timeout)
	p.logger.Debug("This is debug")
	p.logger.Info("This is info")
	p.logger.Warn("This is warn")
	p.logger.Error("This is error")
}

func init() {
	flag.IntVar(&port, "port", 443, "Listening port")
	flag.DurationVar(&timeout, "timeout", 3*time.Second, "Timeout for TLS Handshake initialization")
	flag.StringVar(&logLevelString, "log-level", "INFO", "The log level. One of {DEBUG, INFO, WARN, ERROR}")
}

func main() {
	flag.Parse()
	logLevel := ParseLevel(logLevelString)
	p := NewProxy(port, timeout, logLevel)
	p.Run()
}
