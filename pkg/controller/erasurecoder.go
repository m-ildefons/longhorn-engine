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

type ErasureCoder struct {
	size     uint64
	backends []types.Backend
	code     reedsolomon.Code
}

func NewErasureCoder(n, k int, size uint64, backends []types.Backend) (*ErasureCoder, error) {
	cod, err := reedsolomon.NewCode(n, k)
	if err != nil {
		return nil, err
	}

	// availableBackends := make([]ecbackend, len(backends))
	// for i := range availableBackends {
	// 	availableBackends[i] = ecbackend{0, true, backends[i]}
	// }

	return &ErasureCoder{size, backends, cod}, nil
}

func (e *ErasureCoder) ReadAt(buf []byte, off int64) (int, error) {
	// logrus.Infof("Read of length %d at %d", len(buf), off)
	num, err := e.denseReadAt(buf, off)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (e *ErasureCoder) WriteAt(buf []byte, off int64) (int, error) {
	// logrus.Infof("Write of length %d at %d", len(buf), off)
	num, err := e.denseWriteAt(buf, off)
	if err != nil {
		return 0, err
	}
	return num, nil
}

func (e *ErasureCoder) UnmapAt(length uint32, off int64) (int, error) {
	return 0, ErrNotImplemented
}

func aread(length, offset int64, idx int, backend types.Backend, c chan reedsolomon.Slice, e chan error) {
	buf := make([]byte, length)
	_, err := backend.ReadAt(buf, offset)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		e <- err
		return
	}
	slice := reedsolomon.Slice{
		Index:  idx,
		Length: int(length),
		Data:   buf,
	}
	c <- slice
}

func (e *ErasureCoder) denseReadAt(buf []byte, off int64) (int, error) {
	var err error

	n := int64(e.code.GetN())
	l := int64(len(buf))
	start := off - (off % n)
	reduce := (off + l) % n
	length := off + l + n - reduce - start

	prePadLen := off % n

	sliceOff := start / n
	sliceLen := length / n
	slices := make([]reedsolomon.Slice, n)

	ec := make(chan error, 0)
	ch := make(chan reedsolomon.Slice, len(e.backends))
	for i := 0; i < len(e.backends); i++ {
		// if e.backends[i].generation < e.generation {
		// 	blk := make([]byte, 4096)
		// 	e.backends[i].ReadAt(blk, 0)
		// 	if e.backends[i].responsive {
		// 		logrus.Infof("Need rebuild")
		// 		e.needrebuild = true
		// 		e.rebuild()
		// 	} else {
		// 		logrus.Infof("Backend %d is outdated, ignoring", i)
		// 		continue // avoid reading stale data
		// 	}
		// }

		// dat := make([]byte, sliceLen)
		// _, err = e.backends[i].ReadAt(dat, sliceOff)
		// if err != nil {
		// 	logrus.Infof("Read-error from backend %d: %s", i, err)
		// 	continue
		// }
		go aread(sliceLen, sliceOff, i, e.backends[i], ch, ec)
	}

	sliceIdx := 0
	for i := 0; i < len(e.backends); i++ {
		select {
		case dat := <-ch:
			slices[sliceIdx] = dat
			sliceIdx++
		case err := <-ec:
			logrus.Errorf("%v", err)
			continue
		}

		if int64(sliceIdx) == n {
			break
		}
	}

	if int64(sliceIdx) < n {
		return 0, ErrTooFewSlices
	}

	alignedBuffer, err := e.code.DecodeAligned(slices)
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(l); i++ {
		buf[i] = alignedBuffer[int(prePadLen)+i]
	}
	// logrus.Infof("Aligend Read of length %d at %d", length, start)
	// logrus.Infof("Slice Read of length %d at %d", sliceLen, sliceOff)
	return len(buf), nil
}

func awrite(buffer []byte, offset int64, backend types.Backend, c chan int, e chan error) {
	length, err := backend.WriteAt(buffer, offset)
	if err != nil {
		logrus.Errorf("Error: %v", err)
		e <- err
		return
	}
	c <- length
}

func (e *ErasureCoder) denseWriteAt(buf []byte, off int64) (int, error) {
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
	alignedBuffer := make([]byte, length)
	for i := int64(0); i < prePadLen; i++ {
		alignedBuffer[i] = prePadLine[i]
	}
	for i := int64(0); i < l; i++ {
		alignedBuffer[prePadLen+i] = buf[i]
	}
	for i := postPadLen; i > 0; i-- {
		alignedBuffer[length-i] = postPadLine[n-i]
	}

	slices, err := e.code.EncodeAligned(alignedBuffer)
	if err != nil {
		return 0, err
	}

	ec := make(chan error, 0)
	ch := make(chan int, len(e.backends))
	sliceOff := start / n
	for i := range e.backends {
		//e.backends[i].WriteAt(slices[i].Data, sliceOff)
		go awrite(slices[i].Data, sliceOff, e.backends[i], ch, ec)
	}

	for i := range e.backends {
		select {
		case <-ch:
		case err := <-ec:
			logrus.Errorf("Failed writing to backend %d: %v", i, err)
		}
	}

	// sliceLen := length / n
	// logrus.Infof("Aligend  Write of length %d at %d", length, start)
	// logrus.Infof("Slice Write of length %d at %d", sliceLen, sliceOff)
	return 0, nil
}

func (e *ErasureCoder) blockReadAt(buf []byte, off int64) (int, error) {
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

	sliceBlockRegion := rblk / n
	sliceBlockOffset := sblk / n
	logrus.Infof("reading %d blocks of each slice starting %d", sliceBlockRegion, sliceBlockOffset)

	num, err := e.denseReadAt(buf, off)
	return num, err
}

func (e *ErasureCoder) blockWriteAt(buf []byte, off int64) (int, error) {
	nblk := len(buf) / ECBlockSize
	oblk := off / ECBlockSize
	logrus.Infof("Write of %d blocks starting at block No. %d", nblk, oblk)

	num, err := e.denseWriteAt(buf, off)
	return num, err
}
