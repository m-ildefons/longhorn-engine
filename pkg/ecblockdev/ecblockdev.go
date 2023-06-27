package controller

import (
	"errors"

	"github.com/longhorn/longhorn-engine/pkg/types"
)

const (
	ECBlockSize = 4096
)

var (
	ErrBlockMisalignment = errors.New("Block misalignment")
)

type Block [ECBlockSize]byte

func BufToBlocks(buf []byte) ([]Block, error) {
	if len(buf)%ECBlockSize != 0 {
		return []Block{}, ErrBlockMisalignment
	}

	blocks := make([]Block, len(buf)/ECBlockSize)
	for i := 0; i < len(buf)/ECBlockSize; i++ {
		for j := 0; j < ECBlockSize; j++ {
			blocks[i][j] = buf[i*ECBlockSize+j]
		}
	}

	return []Block{}, nil
}

func BlocksToBuf(blocks []Block) ([]byte, error) {
	buf := make([]byte, len(blocks)*ECBlockSize)
	for i := range blocks {
		for j := 0; j < ECBlockSize; j++ {
			buf[i*ECBlockSize+j] = blocks[i][j]
		}
	}
	return buf, nil
}

type ECBlockDev struct {
	Id      int
	Size    uint64
	backend types.Backend
}

func (dev *ECBlockDev) ReadAt(buf []byte, off int64) (int, error) {
	return dev.backend.ReadAt(buf, off)
}

func (dev *ECBlockDev) WriteAt(buf []byte, off int64) (int, error) {
	return dev.backend.WriteAt(buf, off)
}

func (dev *ECBlockDev) BlockReadAt(buf []Block, off int64) (int, error) {
	return 0, nil
}

func (dev *ECBlockDev) BlockWriteAt(bug []Block, off int64) (int, error) {
	return 0, nil
}
