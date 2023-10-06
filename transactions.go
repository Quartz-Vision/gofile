package gofile

type CommonTxn struct {
	CommonMethods
	file *ManagedFile
}

// Seek changes current read/write position of the file.
// The offset value is relative to the whence position which is file start, current pos or end (0, 1 or 2 respectively)
func (t *CommonTxn) Seek(offset int64, whence int) (ret int64, err error) {
	return t.file.file.Seek(offset, whence)
}

// Size returns current size of the file.
func (t *CommonTxn) Size() (length int64, err error) {
	return t.file.size, nil
}

// Returns the seek position of the file
func (t *CommonTxn) Position() (pos int64) {
	return t.file.pos
}

type managedFileWritableTxn struct {
	Writable
	file *ManagedFile
}

// Write writes the data to a file at the seek position.
//
// It returns the number of bytes written and the error if there is one.
func (t *managedFileWritableTxn) Write(b []byte) (nWritten int64, err error) {
	n, err := t.file.file.Write(b)
	t.file.pos += int64(n)
	if t.file.pos > t.file.size {
		t.file.size = t.file.pos
	}
	return int64(n), err
}

// WriteAt writes bytes to a file at given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "write the last byte of the file".
func (t *managedFileWritableTxn) WriteAt(b []byte, offset int64) (nWritten int64, err error) {
	if offset < 0 {
		offset += t.file.size + 1
	}

	n, err := t.file.file.WriteAt(b, offset)
	newSize := offset + int64(n)
	if err != nil {
		return 0, err
	}
	if newSize > t.file.size {
		t.file.size = newSize
	}
	return int64(n), err
}

// Append appends given data at the end of the file
//
// It returns the position at which the data is written
func (t *managedFileWritableTxn) Append(data []byte) (pos int64, err error) {
	pos = t.file.size
	n, err := t.file.file.WriteAt(data, pos)
	t.file.size += int64(n)
	return pos, err
}

type managedFileReadableTxn struct {
	Readable
	file *ManagedFile
}

// ReadAt reads bytes from a file at a given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "read the last byte of the file".
func (t *managedFileWritableTxn) ReadAt(b []byte, offset int64) (nRead int64, err error) {
	if offset < 0 {
		offset += t.file.size + 1
	}

	n, err := t.file.file.ReadAt(b, offset)
	return int64(n), err
}

// Read reads bytes from a file at the seek position.
//
// It returns the number of bytes read and the error if there is one.
func (t *managedFileWritableTxn) Read(b []byte) (nRead int64, err error) {
	n, err := t.file.file.Read(b)
	t.file.pos += int64(n)
	return int64(n), err
}

type ManagedFileWritableTxn struct {
	CommonTxn
	managedFileWritableTxn
}

type ManagedFileReadableTxn struct {
	CommonTxn
	managedFileReadableTxn
}

type ManagedFileRWTxn struct {
	CommonTxn
	managedFileWritableTxn
	managedFileReadableTxn
}
