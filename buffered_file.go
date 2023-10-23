package gofile

import (
	"errors"
	"io"
)

const POS_REWIND = -1

var ErrOutOfBound = errors.New("requested slice is out of the file's bounds")

// A BufferedFile can be used to wrap any File compliant object and make buffered calls to it.
type BufferedFile struct {
	File

	file            File
	currentPosition int64
	buf             []byte
	BufferSize      int64
	BufferDataSize  int64
}

// NewBufferedFile wraps existing File with BufferedFile and sets given bufferSize to it.
func NewBufferedFile(file File, bufferSize int64) (b *BufferedFile, err error) {
	b = &BufferedFile{
		file:            file,
		currentPosition: POS_REWIND,
		buf:             make([]byte, bufferSize),
		BufferSize:      bufferSize,
		BufferDataSize:  0,
	}

	return b, nil
}

// Seek changes current read/write position of the file.
// The offset value is relative to the whence position which is file start, current pos or end (0, 1 or 2 respectively)
func (b *BufferedFile) Seek(offset int64, whence int) (ret int64, err error) {
	return b.file.Seek(offset, whence)
}

// Size returns current size of the file.
func (b *BufferedFile) Size() (length int64, err error) {
	return b.file.Size()
}

// Returns the seek position of the file
func (b *BufferedFile) Position() (pos int64) {
	return b.file.Position()
}

// Trunc truncates given file and sets it's size to 0.
func (b *BufferedFile) Trunc() (err error) {
	err = b.file.Trunc()
	b.currentPosition = POS_REWIND
	return err
}

// ReadAt reads bytes from a file at given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "read the last byte of the file".
func (b *BufferedFile) ReadAt(data []byte, offset int64) (nRead int64, err error) {
	relativePos := offset - b.currentPosition
	length := int64(len(data))
	readEnd := relativePos + length
	var totalCopied int64 = 0

	if b.currentPosition != POS_REWIND && relativePos < b.BufferDataSize {
		if relativePos >= 0 {
			if readEnd <= b.BufferDataSize {
				return int64(copy(data, b.buf[relativePos:b.BufferDataSize])), nil
			}
			nCopied := int64(copy(data, b.buf[relativePos:b.BufferDataSize]))
			data = data[nCopied:]
			offset += nCopied
			length -= nCopied
			totalCopied += nCopied
		} else {
			if readEnd <= b.BufferDataSize && readEnd > 0 {
				nCopied := int64(copy(data[-relativePos:], b.buf[:b.BufferDataSize]))
				length -= nCopied
				data = data[:length]
				totalCopied += nCopied
			}
		}
	}

	if length <= b.BufferSize {
		nRead, err = b.file.ReadAt(b.buf, offset)
		b.BufferDataSize = nRead
		b.currentPosition = offset

		totalCopied += int64(copy(data, b.buf))
	} else {
		nRead, err = b.file.ReadAt(data, offset)
		if err == io.EOF {
			err = nil
		}
		totalCopied += nRead
		b.BufferDataSize = min(nRead, b.BufferSize)
		bufOffset := nRead - b.BufferDataSize
		b.currentPosition = offset + bufOffset
		copy(b.buf, data[bufOffset:])
	}

	return totalCopied, err
}

// Read reads bytes from a file at the seek position.
//
// It returns the number of bytes read and the error if there is one.
func (b *BufferedFile) Read(data []byte) (nRead int64, err error) {
	offset := b.file.Position()
	nRead, err = b.ReadAt(data, offset)
	b.file.Seek(offset+nRead, io.SeekStart)

	return nRead, err
}

// Write writes the data to a file at the seek position.
//
// It returns the number of bytes written and the error if there is one.
func (b *BufferedFile) Write(data []byte) (nWritten int64, err error) {
	nWritten, err = b.file.Write(data)
	b.currentPosition = POS_REWIND
	return nWritten, err
}

// WriteAt writes bytes to a file at given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "write the last byte of the file".
func (b *BufferedFile) WriteAt(data []byte, offset int64) (nWritten int64, err error) {
	nWritten, err = b.file.WriteAt(data, offset)
	if err != nil {
		return 0, err
	}

	if b.currentPosition <= (offset+int64(nWritten)) && (b.currentPosition+b.BufferSize) >= offset {
		// reset the buffer if its current value overlaps with appended data
		b.currentPosition = POS_REWIND
	}

	return nWritten, err
}

// Append appends given data at the end of the file
func (b *BufferedFile) Append(data []byte) (pos int64, err error) {
	pos, err = b.file.Append(data)
	if err != nil {
		return 0, err
	}

	if (b.currentPosition + b.BufferSize) > pos {
		// reset the buffer if its current value overlaps with appended data
		b.currentPosition = POS_REWIND
	}

	return pos, err
}

// TRead executes given function m locking the file for writing.
// You can only read from this transaction.
//
// NOT BUFFERED YET
func (b *BufferedFile) TRead(m func(txn Readable) (err error)) (err error) {
	err = b.file.TRead(m)
	return err
}

// TWrite executes given function m locking the file for writing from other places.
// You can only write from this transaction.
//
// NOT BUFFERED YET
func (b *BufferedFile) TWrite(m func(txn Writable) (err error)) (err error) {
	err = b.file.TWrite(m)
	b.currentPosition = POS_REWIND
	return err
}

// TReadWrite executes given function m locking the file for writing/reading from other places.
//
// NOT BUFFERED YET
func (b *BufferedFile) TReadWrite(m func(txn Editable) (err error)) (err error) {
	err = b.file.TReadWrite(m)
	b.currentPosition = POS_REWIND
	return err
}

// PipeTo pipes (copies) the data from start of the file and append it to another (dest) by chunks of size chunkSize.
// NOT BUFFERED YET
func (b *BufferedFile) PipeTo(dest Pipeable, chunkSize int64) (err error) {
	return b.file.PipeTo(dest, chunkSize)
}

// Close closes the file (you can reopen it later).
func (b *BufferedFile) Close() {
	b.file.Close()
}
