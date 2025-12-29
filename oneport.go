package oneport

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

// MatchWriter is match that can also write (say to do handshake).
type MatchWriter func(io.Writer, io.Reader) bool

// ErrorHandler handles error and returns whether
// the mux should continue serving the listener.
type ErrorHandler func(error) bool

var _ net.Error = ErrNotMatched{}

// ErrNotMatched is returned whenever a connection is not matched by any
// of the matchers registered in the multiplexer.
type ErrNotMatched struct {
	c net.Conn
}

func (e ErrNotMatched) Error() string {
	return fmt.Sprintf("mux: connection %v not matched by any matchers",
		e.c.RemoteAddr())
}

// Temporary implements net.Error interface.
func (e ErrNotMatched) Temporary() bool { return true }

// Timeout implements net.Error interface.
func (e ErrNotMatched) Timeout() bool { return false }

type errListenerClosed string

func (e errListenerClosed) Error() string   { return string(e) }
func (e errListenerClosed) Temporary() bool { return false }
func (e errListenerClosed) Timeout() bool   { return false }

// ErrListenerClosed  is returned from muxListener.Accept when the underlying listener is closed.
var ErrListenerClosed = errListenerClosed("mux: listener closed")

// ErrServerClosed is returned from muxListener.Accept when the server is closed.
var ErrServerClosed = errors.New("mux: server closed")

// for readability of readTimeout
var noTimeout time.Duration

type matchersLisener struct {
	ss []MatchWriter
	l  muxListener
}

type oneport struct {
	root        net.Listener
	bufLen      int
	errh        ErrorHandler
	sls         []matchersLisener
	readTimeout time.Duration
	donec       chan struct{}
	mu          sync.Mutex
}

func New(l net.Listener) *oneport {
	return &oneport{
		root:        l,
		bufLen:      1024,
		errh:        func(_ error) bool { return true },
		readTimeout: noTimeout,
	}
}

func matchersToMatchWriters(matchers []Matcher) []MatchWriter {
	mws := make([]MatchWriter, 0, len(matchers))
	for _, m := range matchers {
		cm := m
		mws = append(mws, func(w io.Writer, r io.Reader) bool {
			return cm(r)
		})
	}
	return mws
}

func (p *oneport) Match(matchers ...Matcher) net.Listener {
	mws := matchersToMatchWriters(matchers)
	return p.MatchWithWriters(mws...)
}

func (p *oneport) MatchWithWriters(matchers ...MatchWriter) net.Listener {
	ml := muxListener{
		Listener: p.root,
		connc:    make(chan net.Conn, p.bufLen),
		donec:    make(chan struct{}),
	}
	p.sls = append(p.sls, matchersLisener{ss: matchers, l: ml})
	return ml
}

func (p *oneport) SetReadTimeout(t time.Duration) {
	p.readTimeout = t
}

func (p *oneport) Serve() error {
	var wg sync.WaitGroup

	defer func() {
		p.closeDoneChans()
		wg.Wait()

		for _, sl := range p.sls {
			close(sl.l.connc)
			// Drain the connections enqueued for the listener.
			for c := range sl.l.connc {
				_ = c.Close()
			}
		}
	}()

	for {
		conn, err := p.root.Accept()
		if err != nil {
			if !p.handleErr(err) {
				return err
			}
			continue
		}

		wg.Add(1)
		go p.serve(conn, p.donec, &wg)
	}
}

func (p *oneport) serve(conn net.Conn, donec <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	mconn := newMuxConn(conn)
	if p.readTimeout > noTimeout {
		_ = conn.SetReadDeadline(time.Now().Add(p.readTimeout))
	}
	for _, sl := range p.sls {
		for _, s := range sl.ss {
			matched := s(mconn.Conn, mconn.startSniffing())
			if matched {
				mconn.doneSniffing()
				if p.readTimeout > noTimeout {
					_ = conn.SetReadDeadline(time.Time{})
				}
				select {
				case sl.l.connc <- mconn:
				case <-donec:
					_ = conn.Close()
				}
				return
			}
		}
	}
	_ = conn.Close()
	err := ErrNotMatched{c: conn}
	if !p.handleErr(err) {
		_ = p.root.Close()
	}
}

func (p *oneport) Close() {
	p.closeDoneChans()
}

func (p *oneport) closeDoneChans() {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case <-p.donec:
	// Already closed. Don't close again
	default:
		close(p.donec)
	}
	for _, sl := range p.sls {
		select {
		case <-sl.l.donec:
		// Already closed. Don't close again
		default:
			close(sl.l.donec)
		}
	}
}

func (p *oneport) HandleError(h ErrorHandler) {
	p.errh = h
}

func (p *oneport) handleErr(err error) bool {
	if !p.errh(err) {
		return false
	}

	if ne, ok := err.(net.Error); ok {
		return ne.Temporary()
	}

	return false
}

type muxListener struct {
	net.Listener
	connc chan net.Conn
	donec chan struct{}
}

func (l muxListener) Accept() (net.Conn, error) {
	select {
	case c, ok := <-l.connc:
		if !ok {
			return nil, ErrListenerClosed
		}
		return c, nil
	case <-l.donec:
		return nil, ErrServerClosed
	}
}

type MuxConn struct {
	net.Conn
	buf bufferedReader
}

func newMuxConn(conn net.Conn) *MuxConn {
	return &MuxConn{
		Conn: conn,
		buf:  bufferedReader{source: conn},
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
