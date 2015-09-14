// Package tsz implement time-series compression
/*

http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

*/
package tsz

import (
	"bytes"
	"math"

	"github.com/dgryski/go-bits"
	"github.com/dgryski/go-bitstream"
)

type Series struct {
	val float64

	leading  uint64
	trailing uint64

	buf bytes.Buffer
	bw  *bitstream.BitWriter

	first    bool
	finished bool
}

func New() *Series {
	s := Series{
		first:   true,
		leading: ^uint64(0),
	}

	s.bw = bitstream.NewWriter(&s.buf)

	return &s

}

func (s *Series) Bytes() []byte {
	return s.buf.Bytes()
}

func (s *Series) Finish() {

	if !s.finished {
		// // write an end-of-stream record
		// s.bw.WriteBits(0x0f, 4)
		// s.bw.WriteBits(0xffffffff, 32)
		// s.bw.WriteBit(bitstream.Zero)
		s.bw.Flush(bitstream.Zero)
		s.finished = true
	}
}

func (s *Series) Push(v float64) {

	if s.first {
		// first point
		s.val = v
		s.first = false
		s.bw.WriteBits(math.Float64bits(v), 64)
		return
	}

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val)

	if vDelta == 0 {
		s.bw.WriteBit(bitstream.Zero)
	} else {
		s.bw.WriteBit(bitstream.One)

		leading := bits.Clz(vDelta)
		trailing := bits.Ctz(vDelta)

		// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
		if s.leading != ^uint64(0) && leading >= s.leading && trailing >= s.trailing {
			s.bw.WriteBit(bitstream.Zero)
			s.bw.WriteBits(vDelta>>s.trailing, 64-int(s.leading)-int(s.trailing))
		} else {
			s.leading, s.trailing = leading, trailing

			s.bw.WriteBit(bitstream.One)
			s.bw.WriteBits(leading, 5)

			sigbits := 64 - leading - trailing
			s.bw.WriteBits(sigbits, 6)
			s.bw.WriteBits(vDelta>>trailing, int(sigbits))
		}
	}

	s.val = v
}

func (s *Series) Iter() *Iter {
	iter, _ := NewIterator(s.buf.Bytes())
	return iter
}

type Iter struct {
	val float64

	leading  uint64
	trailing uint64

	br *bitstream.BitReader

	b []byte

	first    bool
	finished bool

	err error
}

func NewIterator(b []byte) (*Iter, error) {
	br := bitstream.NewReader(bytes.NewReader(b))

	v, err := br.ReadBits(64)
	if err != nil {
		return nil, err
	}

	return &Iter{
		val:   math.Float64frombits(v),
		first: true,
		br:    br,
		b:     b,
	}, nil
}

func (it *Iter) Next() bool {
	if it.err != nil || it.finished {
		return false
	}

	if it.first {
		it.first = false
		return true
	}

	// read compressed value
	bit, err := it.br.ReadBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == bitstream.Zero {
		// it.val = it.val
	} else {
		bit, err := it.br.ReadBit()
		if err != nil {
			it.err = err
			return false
		}
		if bit == bitstream.Zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.ReadBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = bits

			bits, err = it.br.ReadBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := bits
			it.trailing = 64 - it.leading - mbits
		}

		mbits := int(64 - it.leading - it.trailing)
		bits, err := it.br.ReadBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)
		vbits ^= (bits << it.trailing)
		it.val = math.Float64frombits(vbits)
	}

	return true
}

func (it *Iter) Values() float64 {
	return it.val
}

func (it *Iter) Err() error {
	return it.err
}
