package reedsolomon

import (
	"errors"

	"github.com/sirupsen/logrus"
)

/* Reed-Solomon Coding
 *
 * This package implements Reed-Solomon coding. More specifically, it implemets
 * RS coding suitable for use in Longhorn.
 *
 * A ReedSolomonCode is an object containing all necessary information to
 * encode/decode a byte array to a list of ReedSolomonSlices. A ReedSolomonSlice
 * is a collection of bytes all with the same code index.
 * When encoding n bytes, the result is n+k bytes (polys), each one carrying an
 * index 0..n+k. When encoding i*n bytes, there will be i bytes for each index.
 * A ReedSolomonSlice will contain all bytes with the same index.
 *
 * E.g. for an 3+2 RS code, the bytes 'hello world' will be divided as such:
 *
 *  Slice   1    2    3    4    5
 *  -----------------------------
 *        'h'  'e'  'l'  par  par
 *        'l'  'o'  ' '  par  par
 *        'w'  'o'  'r'  par  par
 *        'l'  'd'  pad  par  par
 *
 * Here 'par' is a parity byte as calulated by Reed-Solomon's algorithm and
 * 'pad' is a padding byte, needed to align the actual message with the
 * boundaries of the slices.
 * Up to k slices can be lost entirely, still allowing reconstruction of the
 * original data and by extension recovery of the lost slice as well.
 *
 * The slices are intended to be distributed onto Longhorn replicas such that
 * each replica receives one slice, thus allowing for the loss of up to k
 * replicas while being able to read/write data.
 */

const (
	// sufficient for n+k < 256
	M = poly(8)
	// generating polynom: x^8+x^6+x^5+x^2+1
	// TODO: proof that this is actually a generating polynom
	P = poly(357)
)

var (
	ErrMisaligned    = errors.New("byte array misaligned")
	ErrTooFewSlices  = errors.New("too few slices for data recovery")
	ErrSliceMismatch = errors.New("slice mismatch")
)

type ReedSolomonCode struct {
	field GaloisField
	n, k  int
	mtx   [][]poly
}

func NewReedSolomonCode(n, k int) (ReedSolomonCode, error) {
	f := GaloisField{M, P}
	logrus.Infof("Using Galois field GF(2^%d) and generating polynom %#b", f.m, f.p)
	mat, err := f.mtx_xform_vandermonde(n, k)
	if err != nil {
		return ReedSolomonCode{}, err
	}
	logrus.Infof("Created %d + %d Reed-Solomon code", n, k)
	return ReedSolomonCode{f, n, k, mat}, nil
}

func (c *ReedSolomonCode) GetN() int { return c.n }

// Encode an aligned byte arrary into a list of slices with data and parity
// bytes.
// Aligned in this context means that the number of bytes is divisible by the
// number of data-slices of the Reed-Solomon code.
func (c *ReedSolomonCode) EncodeAligned(buf []byte) ([]ReedSolomonSlice, error) {
	if len(buf)%c.n != 0 {
		return []ReedSolomonSlice{}, ErrMisaligned
	}
	slices := make([]ReedSolomonSlice, c.n+c.k)
	for i := range slices {
		data := make([]byte, len(buf)/c.n)
		slices[i] = ReedSolomonSlice{i, len(buf) / c.n, data}
	}

	vec := make([]poly, c.n)
	for i := 0; i < len(buf)/c.n; i++ {
		for j := 0; j < c.n; j++ {
			vec[j] = poly(buf[i*c.n+j])
		}

		cod, err := c.field.mtx_vec_dot(c.mtx, vec)
		if err != nil {
			return []ReedSolomonSlice{}, err
		}

		for j := 0; j < c.n+c.k; j++ {
			slices[j].Data[i] = byte(cod[j])
		}
	}

	return slices, nil
}

// Decode a list of slices into an aligned byte array.
// Aligned in this context means that the byte array may be padded with zero
// bytes until it's length is divisible by the number of data-slices of the
// Reed-Solomon code.
func (c *ReedSolomonCode) DecodeAligned(slices []ReedSolomonSlice) ([]byte, error) {
	if len(slices) < c.n {
		return []byte{}, ErrTooFewSlices
	}

	mtx, err := c.buildMatrix(slices)
	if err != nil {
		return []byte{}, err
	}

	bytes := make([]byte, c.n*len(slices[0].Data))
	for i := 0; i < len(slices[0].Data); i++ {
		vec := make([]poly, c.n)
		for j := 0; j < c.n; j++ {
			vec[j] = poly(slices[j].Data[i])
		}
		dat, err := c.field.mtx_vec_dot(mtx, vec)
		if err != nil {
			return []byte{}, err
		}
		for j := 0; j < len(dat); j++ {
			bytes[i*c.n+j] = byte(dat[j])
		}
	}
	return bytes, nil
}

// Rebuild the n+k slices from a list of n slices
func (c *ReedSolomonCode) Rebuild(slices []ReedSolomonSlice) ([]ReedSolomonSlice, error) {
	if len(slices) < c.n {
		return []ReedSolomonSlice{}, ErrTooFewSlices
	}

	mtx, err := c.buildMatrix(slices)
	if err != nil {
		return []ReedSolomonSlice{}, err
	}

	length := slices[0].Length

	result := make([]ReedSolomonSlice, c.n+c.k)
	for i := range result {
		if i == slices[i].Index {
			if slices[i].Length != length {
				return []ReedSolomonSlice{}, ErrSliceMismatch
			}
			slice := ReedSolomonSlice{i, length, slices[i].Data}
			result[i] = slice
		} else {
			dat := make([]byte, length)
			slice := ReedSolomonSlice{i, length, dat}
			result[i] = slice
		}
	}

	for i := 0; i < len(slices[0].Data); i++ {
		vec := make([]poly, c.n)
		for j := 0; j < c.n; j++ {
			vec[j] = poly(slices[j].Data[i])
		}
		dat, err := c.field.mtx_vec_dot(mtx, vec)
		if err != nil {
			return []ReedSolomonSlice{}, err
		}
		cod, err := c.field.mtx_vec_dot(c.mtx, dat)
		if err != nil {
			return []ReedSolomonSlice{}, err
		}
		for j := range result {
			// re-assign regenerated byte
			if j == result[j].Index && j != slices[j].Index {
				result[j].Data[i] = byte(cod[j])
			}
		}
	}

	return result, nil
}

func (c *ReedSolomonCode) BuildMatrix(slices []ReedSolomonSlice) ([][]poly, error) {
	return c.buildMatrix(slices)
}

func (c *ReedSolomonCode) buildMatrix(slices []ReedSolomonSlice) ([][]poly, error) {
	if len(slices) < c.n {
		return [][]poly{}, ErrTooFewSlices
	}

	idx := make([]int, c.n)
	for i := 0; i < c.n; i++ {
		idx[i] = slices[i].Index
	}

	mtx := make([][]poly, c.n)
	for i := range mtx {
		mtx[i] = make([]poly, c.n)
		for j := range mtx[i] {
			mtx[i][j] = c.mtx[idx[i]][j]
		}
	}

	mtx, err := c.field.mtx_inv(mtx)
	if err != nil {
		return [][]poly{}, err
	}
	return mtx, nil
}

type ReedSolomonSlice struct {
	Index  int
	Length int
	Data   []byte
}
