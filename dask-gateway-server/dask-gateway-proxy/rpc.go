package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/rpc"
)

var errMissingParams = errors.New("request body missing params")

type codec struct {
	logger  *Logger
	lreader *io.LimitedReader
	dec     *json.Decoder
	conn    io.ReadWriteCloser
	lenBuf  []byte
	req     request
}

type request struct {
	Method string           `json:"method"`
	Params *json.RawMessage `json:"params"`
	Id     uint64           `json:"id"`
}

type response struct {
	Id     uint64      `json:"id"`
	Result interface{} `json:"result"`
	Error  interface{} `json:"error"`
}

func (r *request) reset() {
	r.Method = ""
	r.Params = nil
	r.Id = 0
}

func (c *codec) ReadRequestHeader(r *rpc.Request) error {
	c.req.reset()

	if _, err := io.ReadFull(c.conn, c.lenBuf); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(c.lenBuf)
	c.lreader.N = int64(length)

	if err := c.dec.Decode(&c.req); err != nil {
		return err
	}
	r.ServiceMethod = c.req.Method
	r.Seq = c.req.Id
	return nil
}

func (c *codec) ReadRequestBody(x interface{}) error {
	if x == nil {
		return nil
	}
	if c.req.Params == nil {
		return errMissingParams
	}
	return json.Unmarshal(*c.req.Params, &x)
}

func (c *codec) WriteResponse(r *rpc.Response, x interface{}) error {
	resp := response{Id: r.Seq}
	if r.Error != "" {
		resp.Error = r.Error
	} else {
		resp.Result = x
	}

	msg, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint32(c.lenBuf, uint32(len(msg)))
	if _, err := c.conn.Write(c.lenBuf[:]); err != nil {
		return err
	}
	if _, err := c.conn.Write(msg); err != nil {
		return err
	}
	return nil
}

func (c *codec) Close() error {
	return c.conn.Close()
}

func authResponse(conn io.ReadWriteCloser, resp uint32) error {
	var msg [4]byte
	binary.BigEndian.PutUint32(msg[:], resp)
	_, err := conn.Write(msg[:])
	if resp == 0 || err != nil {
		conn.Close()
	}
	return err
}

func ServeRpcConn(token string, logger *Logger, conn io.ReadWriteCloser) {
	tokenBuf := make([]byte, len(token))
	if _, err := io.ReadFull(conn, tokenBuf); err != nil {
		logger.Debug("Invalid message in RPC comm, closing")
		authResponse(conn, 0)
		return
	}
	if token != string(tokenBuf) {
		logger.Debug("Unauthorized connection to RPC comm, closing")
		authResponse(conn, 0)
		return
	}
	if err := authResponse(conn, 1); err != nil {
		return
	}

	lreader := io.LimitedReader{R: conn}
	c := &codec{
		logger:  logger,
		lreader: &lreader,
		dec:     json.NewDecoder(&lreader),
		conn:    conn,
		lenBuf:  make([]byte, 4),
	}
	rpc.ServeCodec(c)
}
