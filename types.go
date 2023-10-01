package gofile

import (
	"errors"
)

var ErrStorageClosed = errors.New("operation is impossible, storage is closed")

type Readable interface {
	ReadAt(b []byte, offset int64) (nRead int64, err error)
	Size() (length int64, err error)
}

type Writable interface {
	Seek(offset int64, whence int) (ret int64, err error)
	Write(b []byte) (nWritten int64, err error)
	WriteAt(b []byte, offset int64) (nWritten int64, err error)
	Append(data []byte) (pos int64, err error)
}

type Editable interface {
	Readable
	Writable
}

type Freeable interface {
	Trunc() (err error)
}

type File interface {
	Editable
	Freeable

	TRead(m func(txn Readable) (err error)) (err error)
	TWrite(m func(txn Writable) (err error)) (err error)
	TReadWrite(m func(txn Editable) (err error)) (err error)
	PipeTo(dest File, chunkSize int64) (err error)

	Close()
}
