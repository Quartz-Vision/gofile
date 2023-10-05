package gofile

type CommonTxn struct {
	CommonMethods
	file *ManagedFile
}

func (t *CommonTxn) Seek(offset int64, whence int) (ret int64, err error) {
	return t.file.file.Seek(offset, whence)
}
func (t *CommonTxn) Size() (length int64, err error) {
	return t.file.size, nil
}
func (t *CommonTxn) Position() (pos int64) {
	return t.file.pos
}

type managedFileWritableTxn struct {
	Writable
	file *ManagedFile
}

func (t *managedFileWritableTxn) Write(b []byte) (nWritten int64, err error) {
	n, err := t.file.file.Write(b)
	t.file.pos += int64(n)
	if t.file.pos > t.file.size {
		t.file.size = t.file.pos
	}
	return int64(n), err
}
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
func (t *managedFileWritableTxn) Append(data []byte) (pos int64, err error) {
	_, err = t.WriteAt(data, t.file.size)
	return t.file.size - int64(len(data)), err
}

type managedFileReadableTxn struct {
	Readable
	file *ManagedFile
}

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
