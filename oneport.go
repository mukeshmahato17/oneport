package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// Matcher matches the connection based on its content.
type Matcher func(io.Reader) bool

// MatchWriter is a match that can also write response (say to do handshake).
type MatchWriter func(io.Writer, io.Reader) bool

// ErrorHandler handles error and returns whether
// the mux should continue serving the 	listener.
type ErrorHandler func(error) bool

var _ net.Error = ErrNotMatched{}

// ErrNotMatched is returned whenever a connection is not matched by any of the
// matchers registered in the multiplexer.
type ErrNotMatched struct {
	conn net.Conn
}

func (e ErrNotMatched) Error() string {
	return fmt.Sprintf("mux: connection %v is not matched by any matcher", e.conn.RemoteAddr())
}

// Temporary() implements the net.Error interface.
func (e ErrNotMatched) Temporary() bool { return true }

// Timeout() implements the net.Error interface.
func (e ErrNotMatched) Timeout() bool { return false }

type errListenerClosed string

func (e errListenerClosed) Error() string   { return string(e) }
func (e errListenerClosed) Temporary() bool { return false }
func (e errListenerClosed) Timeout() bool   { return false }

// ErrListenerClosed is returned from muxListener.Accept() when the
// underlying listener is closed.
var ErrListenerClosed = errListenerClosed("mux: listener closed")

// ErrServerClosed is returned from muxListener.Accept() when the server is closed.
var ErrServerClosed = errors.New("mux: server closed")

// for readibility of readTimeout
var noTimeout time.Duration

type matchersListener struct {
	ss []MatchWriter
	l  muxListener
}

func matchersToMatcheWriters(matchers []Matcher) []MatchWriter {
	mws := make([]MatchWriter, 0, len(matchers))
	for _, m := range matchers {
		cm := m
		mws = append(mws, func(w io.Writer, r io.Reader) bool {
			return cm(r)
		})
	}
	return mws
}

type Oneport struct {
	root        net.Listener
	bufLen      int64
	errorh      ErrorHandler
	sls         []matchersListener
	readTimeout time.Duration
	doneChan    chan struct{}
	mu          sync.Mutex
}

func New(l net.Listener) *Oneport {
	return &Oneport{
		root:        l,
		bufLen:      1024,
		errorh:      func(_ error) bool { return true },
		doneChan:    make(chan struct{}),
		readTimeout: noTimeout,
	}
}

func (p *Oneport) Match(matchers ...Matcher) net.Listener {
	mws := matchersToMatcheWriters(matchers)
	return p.MatchWithWriters(mws...)
}

func (p *Oneport) MatchWithWriters(matchers ...MatchWriter) net.Listener {
	ml := muxListener{
		Listener: p.root,
		doneChan: make(chan struct{}),
		connChan: make(chan net.Conn),
	}
	p.sls = append(p.sls, matchersListener{ss: matchers, l: ml})
	return ml
}

func (p *Oneport) SetTimeout(t time.Duration) {
	p.readTimeout = t
}

func (p *Oneport) Serve() error {
	for {
		conn, err := p.root.Accept()
		if err != nil {
			if !p.handleErr(err) {
				return err
			}
			continue
		}
	}

}

func (p *Oneport) serve(conn net.Conn, doneChan <-chan struct{}, wg *sync.WaitGroup) {
	mconn := newMuxConn(conn)
	if p.readTimeout > noTimeout {
		_ = conn.SetReadDeadline(time.Now().Add(p.readTimeout))
	}

	for {

	}

}

func (p *Oneport) HandleError(h ErrorHandler) {
	p.errorh = h
}

func (p *Oneport) handleErr(err error) bool {
	if !p.errorh(err) {
		return false
	}

	if ne, ok := err.(net.Error); ok {
		return ne.Timeout()
	}

	return false
}

type muxListener struct {
	net.Listener
	connChan chan net.Conn
	doneChan chan struct{}
}

// / MuxConn wraps a net.Conn and provides transparent sniffing of connection data.
type MuxConn struct {
	net.Conn
	buf bufferedReader
}

func newMuxConn(c net.Conn) *MuxConn {
	return &MuxConn{
		Conn: c,
		buf:  bufferedReader{source: c},
	}
}

// From the io.Reader documentation:
//
// When Read encounters an error or end-of-file condition after
// successfully reading n > 0 bytes, it returns the number of
// bytes read.  It may return the (non-nil) error from the same call
// or return the error (and n == 0) from a subsequent call.
// An instance of this general case is that a Reader returning
// a non-zero number of bytes at the end of the input stream may
// return either err == EOF or err == nil.  The next Read should
// return 0, EOF.
func (m *MuxConn) Read(p []byte) (int, error) {
	return m.buf.Read(p)
}

func (m *MuxConn) startSniffing() io.Reader {
	m.buf.reset(true)
	return &m.buf
}

func (m *MuxConn) doneSniffing() {
	m.buf.reset(false)
}
