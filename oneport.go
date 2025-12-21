package main

import (
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
	errorh      ErrorHandler
	sls         []matchersListener
	readTimeout time.Duration
}

func New(l net.Listener) *Oneport {
	return &Oneport{
		root: l,
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

// MuxConn wraps the net.Conn and provide a transparent sniffing of connection data.
type MuxConn struct {
	net.Conn
}

func newMuxConn(conn net.Conn) *MuxConn {
	return &MuxConn{
		Conn: conn,
	}
}
