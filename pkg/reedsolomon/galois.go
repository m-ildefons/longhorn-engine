package reedsolomon

import (
	"errors"
)

var (
	ErrDimensionMismatch     = errors.New("vector or matrix dimension mismatch")
	ErrNoninvertibleMatrix   = errors.New("matrix not invertible")
	ErrZeroDivision          = errors.New("division by zero")
	ErrInsufficientFieldSize = errors.New("size of Galois field insufficient")
)

// elements of the Galois field are polynoms. Must be a type large enough to
// hold 2^m+1 values
type poly uint64

// a good choice for erasure coding would be e.g. GF(2^8), i.e. m=8
// with generating polynom 357 (x^8+x^6+x^5+x^2+1). This allows for encoding
// byte-aligned messages easily as each member of the field fits exactly in one
// byte.
type GaloisField struct {
	m poly // GF(2^m)
	p poly // generating polynom
}

// returns number of elements in the Galois field
func (g *GaloisField) size() uint64 {
	return (1 << g.m)
}

// x + y
func (g *GaloisField) add(x, y poly) poly {
	return poly(x ^ y)
}

// x * y
func (g *GaloisField) mul(x, y poly) poly {
	prd := poly(0)
	pol := poly(g.p ^ (1 << g.m)) // just the low order terms of the generating polynom
	v1 := poly(x)
	v2 := poly(y)

	for i := poly(0); i < g.m; i++ {
		if v1&poly(1) != 0 {
			prd = prd ^ (v2 << i)
		}
		v1 = v1 >> 1
		if v1 == poly(0) {
			break
		}
	}

	mask := poly(1) << (g.m + (g.m - 2))

	// i must be signed or this will loop forever!
	for i := int64(g.m - 2); i >= 0; i-- {
		if prd&mask != 0 {
			prd = prd & (^mask)
			prd = prd ^ (pol << i)
		}
		mask = mask >> 1
	}
	return prd
}

// 1 / x
func (g *GaloisField) inv(x poly) (poly, error) {
	if x == 0 {
		return poly(0), ErrZeroDivision
	}

	for i := poly(1); i < poly(g.size()); i++ {
		if g.mul(x, i) == 1 {
			return i, nil
		}
	}
	return poly(1), nil
}

// x / y
func (g *GaloisField) div(x, y poly) (poly, error) {
	yinv, err := g.inv(y)
	if err != nil {
		return poly(0), err
	}
	return g.mul(x, yinv), nil
}

// b ^ e
func (g *GaloisField) pow(b, e poly) poly {
	pwr := poly(1)
	for i := poly(0); i < e; i++ {
		pwr = g.mul(pwr, b)
	}
	return pwr
}

// v * v
func (g *GaloisField) vec_vec_dot(v1, v2 []poly) (poly, error) {
	if len(v1) != len(v2) {
		return poly(0), ErrDimensionMismatch
	}

	prd := poly(0)
	for i := 0; i < len(v1); i++ {
		prd = g.add(prd, g.mul(v1[i], v2[i]))
	}
	return prd, nil
}

// A * v
func (g *GaloisField) mtx_vec_dot(A [][]poly, v []poly) ([]poly, error) {
	res := make([]poly, len(A))
	for i := range res {
		d, err := g.vec_vec_dot(A[i], v)
		if err != nil {
			return []poly{}, err
		}
		res[i] = d
	}
	return res, nil
}

// generate an identity matrix of dimension n by n
func (g *GaloisField) mtx_identity(n int) ([][]poly, error) {
	if n <= 0 {
		return [][]poly{}, ErrDimensionMismatch
	}

	mtx := make([][]poly, n)
	for i := range mtx {
		mtx[i] = make([]poly, n)
		mtx[i][i] = poly(1)
	}

	return mtx, nil
}

// generate a vandermonde matrix of n+k rows by n columns
func (g *GaloisField) mtx_vandermonde(n, k int) ([][]poly, error) {
	if uint64(n+k) > g.size() {
		return [][]poly{}, ErrInsufficientFieldSize
	}
	vmm := make([][]poly, n+k)
	for i := range vmm {
		vmm[i] = make([]poly, n)
		for j := range vmm[i] {
			vmm[i][j] = g.pow(poly(i), poly(j))
		}
	}
	return vmm, nil
}

// generate a modified vandermonde matrix, where the top rows form an indentity
// matrix, using linear transformations
func (g *GaloisField) mtx_xform_vandermonde(n, k int) ([][]poly, error) {
	mtx, err := g.mtx_vandermonde(n, k)
	if err != nil {
		return mtx, err
	}

	// first row/col is ok by definition
	for i := 1; i < n; i++ {
		// ensure diagonal element is 1 by dividing each element in row by diagonal
		d := mtx[i][i]
		for j := 0; j < n; j++ {
			v, err := g.div(mtx[i][j], d)
			if err != nil {
				return [][]poly{}, err
			}
			mtx[i][j] = v
		}

		// fixup rest of the row
		for j := 0; j < n; j++ {
			if j == i {
				continue // diagonal is ok already
			}

			s := mtx[i][j]
			for l := 0; l < n+k; l++ {
				// addition is also substraction in a Galois field
				mtx[l][j] = g.add(g.mul(s, mtx[l][i]), mtx[l][j])
			}
		}
	}

	return mtx, nil
}

// invert matrix A, if possible
func (g *GaloisField) mtx_inv(A [][]poly) ([][]poly, error) {
	n := len(A)
	if n != len(A[0]) {
		return [][]poly{}, ErrNoninvertibleMatrix
	}

	I, err := g.mtx_identity(n)
	if err != nil {
		return [][]poly{}, err
	}

	for i := 0; i < n; i++ {
		// if diagonal element is 0
		if A[i][i] == 0 {
			for j := 0; j < n; j++ {
				if A[i][j] != 0 {
					for k := 0; k < n; k++ {
						A[k][i] = g.add(A[k][i], A[k][j])
						I[k][i] = g.add(I[k][i], I[k][j])
					}
					break
				}
			}
		}

		// if it's still 0, error out
		if A[i][i] == 0 {
			return [][]poly{}, ErrNoninvertibleMatrix
		}

		// divide column by inverse of diagonal element, if diagonal element isn't 1
		if A[i][i] != 1 {
			v := A[i][i]
			for j := 0; j < n; j++ {
				f1, err := g.div(A[j][i], v)
				if err != nil {
					return [][]poly{}, err
				}
				f2, err := g.div(I[j][i], v)
				if err != nil {
					return [][]poly{}, err
				}
				A[j][i] = f1
				I[j][i] = f2
			}
		}

		// now zero out the resut of the row
		for j := 0; j < n; j++ {
			// skip diagonal and columns that already have a zero in row i
			if j == i || A[i][j] == 0 {
				continue
			}

			v := A[i][j]
			for k := 0; k < n; k++ {
				// note: in a Galois field, addition is substraction too
				A[k][j] = g.add(g.mul(v, A[k][i]), A[k][j])
				I[k][j] = g.add(g.mul(v, I[k][i]), I[k][j])
			}
		}
	}

	return I, nil
}
