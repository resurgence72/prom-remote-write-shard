package pkg

import (
	"io"
	"math/bits"
	"sync"
)

// ByteBuffer implements a simple byte buffer.
type ByteBuffer struct {
	// B is the underlying byte slice.
	B []byte
}

// Reset resets bb.
func (bb *ByteBuffer) Reset() {
	bb.B = bb.B[:0]
}

// Write appends p to bb.
func (bb *ByteBuffer) Write(p []byte) (int, error) {
	bb.B = append(bb.B, p...)
	return len(p), nil
}

func ResizeWithCopyMayOverallocate(b []byte, n int) []byte {
	if n <= cap(b) {
		return b[:n]
	}
	nNew := func(n int) int {
		pow2 := uint8(bits.Len(uint(n - 1)))
		return 1 << pow2
	}(n)
	bNew := make([]byte, nNew)
	copy(bNew, b)
	return bNew[:n]
}

// ReadFrom reads all the data from r to bb until EOF.
func (bb *ByteBuffer) ReadFrom(r io.Reader) (int64, error) {
	b := bb.B
	bLen := len(b)
	b = ResizeWithCopyMayOverallocate(b, 4*1024)
	b = b[:cap(b)]
	offset := bLen
	for {
		if free := len(b) - offset; free < offset {
			n := len(b)
			b = append(b, make([]byte, n)...)
		}
		n, err := r.Read(b[offset:])
		offset += n
		if err != nil {
			bb.B = b[:offset]
			if err == io.EOF {
				err = nil
			}
			return int64(offset - bLen), err
		}
	}
}

// MustClose closes bb for subsequent re-use.
func (bb *ByteBuffer) MustClose() {
	// Do nothing, since certain code rely on bb reading after MustClose call.
}

// ByteBufferPool is a pool of ByteBuffers.
type ByteBufferPool struct {
	p sync.Pool
}

// Get obtains a ByteBuffer from bbp.
func (bbp *ByteBufferPool) Get() *ByteBuffer {
	bbv := bbp.p.Get()
	if bbv == nil {
		return &ByteBuffer{}
	}
	return bbv.(*ByteBuffer)
}

// Put puts bb into bbp.
func (bbp *ByteBufferPool) Put(bb *ByteBuffer) {
	bb.Reset()
	bbp.p.Put(bb)
}
