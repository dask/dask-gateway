package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"net"
)

type tcpConn interface {
	net.Conn
	CloseWrite() error
	CloseRead() error
}

type peekedTCPConn struct {
	peeked []byte
	*net.TCPConn
}

func (c *peekedTCPConn) Read(p []byte) (n int, err error) {
	if len(c.peeked) > 0 {
		n = copy(p, c.peeked)
		c.peeked = c.peeked[n:]
		if len(c.peeked) == 0 {
			c.peeked = nil
		}
		return n, nil
	}
	return c.TCPConn.Read(p)
}

func wrapPeeked(inConn *net.TCPConn, br *bufio.Reader) tcpConn {
	peeked, _ := br.Peek(br.Buffered())
	return &peekedTCPConn{TCPConn: inConn, peeked: peeked}
}

type readonly struct {
	r io.Reader
	net.Conn
}

func (c readonly) Read(p []byte) (int, error) { return c.r.Read(p) }
func (readonly) Write(p []byte) (int, error)  { return 0, io.EOF }

func readSNI(inConn *net.TCPConn) (string, bool, tcpConn, error) {
	br := bufio.NewReader(inConn)
	hdr, err := br.Peek(1)
	if err != nil {
		return "", false, nil, err
	}

	if hdr[0] != 0x16 {
		// Not a TLS handshake
		return "", false, wrapPeeked(inConn, br), nil
	}

	const headerLen = 5
	hdr, err = br.Peek(headerLen)
	if err != nil {
		return "", false, wrapPeeked(inConn, br), nil
	}

	recLen := int(hdr[3])<<8 | int(hdr[4])
	helloBytes, err := br.Peek(headerLen + recLen)
	if err != nil {
		return "", true, wrapPeeked(inConn, br), nil
	}

	sni := ""
	server := tls.Server(readonly{r: bytes.NewReader(helloBytes)}, &tls.Config{
		GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			sni = hello.ServerName
			return nil, nil
		},
	})
	server.Handshake()

	return sni, true, wrapPeeked(inConn, br), nil
}
