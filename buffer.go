package oneport

import (
	"bytes"
	"io"
)

// bufferReader is an optimized implementation of io.Reader that behaves like
// io.MultiReader(bytes.NewReader(buffer.Bytes()), io.TeeReader(source, buffer))
// witout allocating.
type bufferedReader struct {
	source     io.Reader
	buffer     bytes.Buffer
	bufferRead int
	bufferSize int
	sniffing   bool
	lastErr    error
}

func (r *bufferedReader) Read(p []byte) (int, error) {
	if r.bufferSize > r.bufferRead {
		// If we have already read something  from the buffer before, we retuen the
		// same data and the last error if any. We need to immediately reutrn,
		// otherwise we may block forever, if we try to be smart and call
		// source.Read() seeking a little bit of more data.
		bn := copy(p, r.buffer.Bytes()[r.bufferRead:r.bufferSize])
		r.bufferRead += bn
		return bn, r.lastErr
	} else if !r.sniffing && r.buffer.Cap() != 0 {
		// We don't need the buffer anymore.
		// Reset it to release the internal slice.
		r.buffer = bytes.Buffer{}
	}
	// If there is nothing more to return in sniffed buffer, read from
	// the source.
	sn, sErr := r.source.Read(p)
	if sn > 0 && r.sniffing {
		r.lastErr = sErr
		if wn, wErr := r.buffer.Write(p[:sn]); wErr != nil {
			return wn, wErr
		}
	}
	return sn, sErr
}

func (r *bufferedReader) reset(snif bool) {
	r.sniffing = snif
	r.bufferRead = 0
	r.bufferSize = r.buffer.Len()
}
