package controller

import (
	"errors"

	"github.com/longhorn/longhorn-engine/pkg/reedsolomon"
	"github.com/longhorn/longhorn-engine/pkg/types"
	"github.com/sirupsen/logrus"
)

const (
	ECBlockSize = 4096
)

var (
	ErrTooFewSlices      = errors.New("Too few slices available")
	ErrNotImplemented    = errors.New("Not yet implemented")
	ErrBlockMisalignment = errors.New("Block misalignment")
	ErrRebuildInProgress = errors.New("Rebuild in progress")
)

type erasurecoder struct {
	size     uint64
	backends []types.Backend
	code     reedsolomon.ReedSolomonCode
}

func NewErasureCoder(n, k int, size uint64, backends []types.Backend) (*erasurecoder, error) {
	cod, err := reedsolomon.NewReedSolomonCode(n, k)
	if err != nil {
		return nil, err
	}

	return &erasurecoder{size, backends, cod}, nil
}

func (e *erasurecoder) ReadAt(buf []byte, off int64) (int, error) {
	logrus.Infof("Read of length %d at %d", len(buf), off)
	num, err := e.denseReadAt(buf, off)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (e *erasurecoder) WriteAt(buf []byte, off int64) (int, error) {
	logrus.Infof("Write of length %d at %d", len(buf), off)
	num, err := e.denseWriteAt(buf, off)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (e *erasurecoder) UnmapAt(length uint32, off int64) (int, error) {
	return 0, ErrNotImplemented
}

func (e *erasurecoder) denseReadAt(buf []byte, off int64) (int, error) {
	var err error

	n := int64(e.code.GetN())
	l := int64(len(buf))
	start := off - (off % n)
	reduce := (off + l) % n
	length := off + l + n - reduce - start

	prePadLen := off % n

	sliceOff := start / n
	sliceLen := length / n
	slices := make([]reedsolomon.ReedSolomonSlice, n)

	sliceIdx := 0
	for i := 0; i < len(e.backends); i++ {
		dat := make([]byte, sliceLen)
		_, err = e.backends[i].ReadAt(dat, sliceOff)
		if err != nil {
			logrus.Infof("Read-error from backend %d: %s", i, err)
			continue
		}
		slices[sliceIdx] = reedsolomon.ReedSolomonSlice{Index: i, Length: int(sliceLen), Data: dat}

		sliceIdx++
		if int64(sliceIdx) == n {
			break
		}
	}

	if int64(sliceIdx) < n {
		return 0, ErrTooFewSlices
	}

	aligned_buffer, err := e.code.DecodeAligned(slices)
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(l); i++ {
		buf[i] = aligned_buffer[int(prePadLen)+i]
	}
	// logrus.Infof("Aligend Read of length %d at %d", length, start)
	// logrus.Infof("Slice Read of length %d at %d", sliceLen, sliceOff)
	return len(buf), nil
}

func (e *erasurecoder) denseWriteAt(buf []byte, off int64) (int, error) {
	var err error
	n := int64(e.code.GetN())
	l := int64(len(buf))
	start := off - (off % n)
	reduce := (off + l) % n
	length := off + l + n - reduce - start

	prePadLine := make([]byte, n)
	_, err = e.ReadAt(prePadLine, start)
	if err != nil {
		return 0, err
	}
	postPadLine := make([]byte, n)
	_, err = e.ReadAt(postPadLine, start+length-n)
	if err != nil {
		return 0, err
	}

	prePadLen := off % n
	postPadLen := length - prePadLen - l
	aligned_buffer := make([]byte, length)
	for i := int64(0); i < prePadLen; i++ {
		aligned_buffer[i] = prePadLine[i]
	}
	for i := int64(0); i < l; i++ {
		aligned_buffer[prePadLen+i] = buf[i]
	}
	for i := postPadLen; i > 0; i-- {
		aligned_buffer[length-i] = postPadLine[n-i]
	}

	slices, err := e.code.EncodeAligned(aligned_buffer)
	if err != nil {
		return 0, err
	}

	sliceOff := start / n
	for i := range e.backends {
		e.backends[i].WriteAt(slices[i].Data, sliceOff)
	}

	// sliceLen := length / n
	// logrus.Infof("Aligend  Write of length %d at %d", length, start)
	// logrus.Infof("Slice Write of length %d at %d", sliceLen, sliceOff)
	return 0, nil
}

func (e *erasurecoder) blockReadAt(buf []byte, off int64) (int, error) {
	nblk := int64(len(buf) / ECBlockSize)
	oblk := off / ECBlockSize
	//logrus.Infof("Read of %d blocks starting at block No. %d", nblk, oblk)

	n := int64(e.code.GetN())

	hblk := oblk % n              // number of blocks to ignore at head
	tblk := (oblk + nblk + n) % n // number of blocks to ignore at end
	sblk := oblk - hblk           // block index of block to start reading
	eblk := oblk + nblk + tblk    // block index of last block to read
	rblk := eblk - sblk           // block region, number of blocks to read in total

	logrus.Infof("Read of %d blocks at index %d. "+
		"Actual read of %d blocks starting at %d until %d, "+
		"with head and tail of %d and %d", nblk, oblk, rblk, sblk, eblk, hblk, tblk)

	slice_block_region := rblk / n
	slice_block_offset := sblk / n
	logrus.Infof("reading %d blocks of each slice starting %d", slice_block_region, slice_block_offset)

	num, err := e.denseReadAt(buf, off)
	return num, err
}

func (e *erasurecoder) blockWriteAt(buf []byte, off int64) (int, error) {
	nblk := len(buf) / ECBlockSize
	oblk := off / ECBlockSize
	logrus.Infof("Write of %d blocks starting at block No. %d", nblk, oblk)

	num, err := e.denseWriteAt(buf, off)
	return num, err
}
