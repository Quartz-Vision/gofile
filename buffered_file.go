package gofile

import (
	"errors"
	"io"
)

const POS_REWIND = -1

var ErrOutOfBound = errors.New("requested slice is out of the file's bounds")

type BufferedFile struct {
	File

	file           File
	currentPostion int64
	buf            []byte
	BufferSize     int64
}

func NewBufferedFile(file File, bufferSize int64) (b *BufferedFile, err error) {
	b = &BufferedFile{
		file:           file,
		currentPostion: POS_REWIND,
		buf:            make([]byte, bufferSize),
		BufferSize:     bufferSize,
	}

	return b, nil
}
func (b *BufferedFile) Trunc() (err error) {
	err = b.file.Trunc()
	b.currentPostion = POS_REWIND
	return err
}
func (b *BufferedFile) ReadAt(data []byte, offset int64) (nRead int64, err error) {
	relativePos := offset - b.currentPostion
	length := int64(len(data))

	// What this checks:
	// offset >= b.currentPostion && (offset + length) <= (b.BufferSize + b.currentPostion)
	// -b.currentPostion ==>
	// offset - b.currentPostion >= 0 && (offset + length - b.currentPostion) <= b.BufferSize
	// relativePos = offset - b.currentPostion ==>
	// relativePos >= 0 && (relativePos+length) <= b.BufferSize
	if b.currentPostion != POS_REWIND && relativePos >= 0 && (relativePos+length) <= b.BufferSize {
		copy(data, b.buf[relativePos:])
		return length, nil
	}

	_, err = b.file.ReadAt(b.buf, offset)
	if err == io.EOF {
		err = nil
	}
	b.currentPostion = offset

	copy(data, b.buf)
	return length, err
}

func (b *BufferedFile) Seek(offset int64, whence int) (ret int64, err error) {
	return b.file.Seek(offset, whence)
}

func (b *BufferedFile) Write(data []byte) (nWritten int64, err error) {
	nWritten, err = b.file.Write(data)
	b.currentPostion = POS_REWIND
	return nWritten, err
}

func (b *BufferedFile) WriteAt(data []byte, offset int64) (nWritten int64, err error) {
	nWritten, err = b.file.WriteAt(data, offset)
	if err != nil {
		return 0, err
	}

	if b.currentPostion <= (offset+int64(nWritten)) && (b.currentPostion+b.BufferSize) >= offset {
		// reset the buffer if its current value overlaps with appended data
		b.currentPostion = POS_REWIND
	}

	return nWritten, err
}

func (b *BufferedFile) Append(data []byte) (pos int64, err error) {
	pos, err = b.file.Append(data)
	if err != nil {
		return 0, err
	}

	if (b.currentPostion + b.BufferSize) > pos {
		// reset the buffer if its current value overlaps with appended data
		b.currentPostion = POS_REWIND
	}

	return pos, err
}

func (b *BufferedFile) TRead(m func(txn Readable) (err error)) (err error) {
	err = b.file.TRead(m)
	return err
}

func (b *BufferedFile) TWrite(m func(txn Writable) (err error)) (err error) {
	err = b.file.TWrite(m)
	b.currentPostion = POS_REWIND
	return err
}

func (b *BufferedFile) TReadWrite(m func(txn Editable) (err error)) (err error) {
	err = b.file.TReadWrite(m)
	b.currentPostion = POS_REWIND
	return err
}

func (b *BufferedFile) PipeTo(dest File, chunkSize int64) (err error) {
	return b.file.PipeTo(dest, chunkSize)
}

func (b *BufferedFile) Size() (length int64, err error) {
	return b.file.Size()
}

func (b *BufferedFile) Close() {
	b.file.Close()
}
