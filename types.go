package gofile

type CommonMethods interface {
	Seek(offset int64, whence int) (ret int64, err error)
	Size() (length int64, err error)
	Position() (pos int64)
}

// Readable objects should support reading operations
type Readable interface {
	CommonMethods

	ReadAt(b []byte, offset int64) (nRead int64, err error)
	Read(b []byte) (nRead int64, err error)
}

// Writable objects should support writing operations
type Writable interface {
	CommonMethods

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

type Pipeable interface {
	Transactional
	Writable
}

// Transactional objects support methods that create any transactions
type Transactional interface {
	TRead(m func(txn Readable) (err error)) (err error)
	TWrite(m func(txn Writable) (err error)) (err error)
	TReadWrite(m func(txn Editable) (err error)) (err error)
	PipeTo(dest Pipeable, chunkSize int64) (err error)
}

type File interface {
	Editable
	Freeable
	Transactional

	Close()
}
