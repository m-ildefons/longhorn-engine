package reedsolomon

/* Reed-Solomon Coding
 *
 * This is an alternative coding scheme for block-aligned Reed-Solomon
 * encodings.
 *
 * For example for the message 'hello world!', with a block size of 2 and three
 * data slices and two parity slices, the following data layout would be
 * achieved:
 *
 * Slice   1    2    3    4    5
 * -----------------------------
 *       'h'  'l'  'o'  par  par
 *       'e'  'l'  ' '  par  par
 *
 *       par  'w'  'r'  'd'  par
 *       par  'o'  'l'  '!'  par
 *
 * This allows optimized reads/writes if the IO operation is already
 * block-aligned
 */

const (
	BlockSize = 4096
)

type Block [BlockSize]byte

func BytesToBlocks(buf []byte) ([]Block, error) {
	if remainder := len(buf) % BlockSize; remainder != 0 {
		buf2 := make([]byte, len(buf)+(4096-remainder))
		for i := 0; i < len(buf); i++ {
			buf2[i] = buf[i]
			return BytesToBlocksAligned(buf2)
		}
	}
	return BytesToBlocksAligned(buf)
}

func BytesToBlocksAligned(buf []byte) ([]Block, error) {
	if len(buf)%BlockSize != 0 {
		return []Block{}, ErrMisaligned
	}

	blocks := make([]Block, len(buf)/BlockSize+1)
	for i := int64(0); i < int64(len(buf)/BlockSize+1); i++ {
		for j := int64(0); j < BlockSize; j++ {
			blocks[i][j] = buf[i*BlockSize+j]
		}
	}
	return blocks, nil
}

func BlocksToBytes(blocks []Block) ([]byte, error) {
	buf := make([]byte, len(blocks)*BlockSize)
	for i := int64(0); i < int64(len(blocks)); i++ {
		for j := int64(0); j < int64(BlockSize); j++ {
			buf[i*BlockSize+j] = blocks[i][j]
		}
	}
	return buf, nil
}

func (c *Code) EncodeBlocks(blocks []Block) ([]Slice, error) {
	return []Slice{}, nil
}

func (c *Code) DecodeBlocks(slices []Slice) ([]Block, error) {
	return []Block{}, nil
}

func (c *Code) RebuildBlocks(slices []Slice) ([]Slice, error) {
	return []Slice{}, nil
}
