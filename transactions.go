package gofile

type ManagedFileWritableTxn struct {
	Writable
	file *ManagedFile
}

func (t *ManagedFileWritableTxn) Seek(offset int64, whence int) (ret int64, err error) {
	return t.file.file.Seek(offset, whence)
}
func (t *ManagedFileWritableTxn) Write(b []byte) (nWritten int64, err error) {
	n, err := t.file.file.Write(b)
	newSize := t.file.pos + int64(n)
	if newSize > t.file.size {
		t.file.size = newSize
	}
	return int64(n), err
}
func (t *ManagedFileWritableTxn) WriteAt(b []byte, offset int64) (nWritten int64, err error) {
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
func (t *ManagedFileWritableTxn) Append(data []byte) (pos int64, err error) {
	_, err = t.WriteAt(data, t.file.size)
	return t.file.size - int64(len(data)), err
}

type ManagedFileReadableTxn struct {
	Readable
	file *ManagedFile
}

func (t *ManagedFileReadableTxn) Size() (length int64, err error) {
	return t.file.size, nil
}
func (t *ManagedFileReadableTxn) ReadAt(b []byte, offset int64) (nRead int64, err error) {
	if offset < 0 {
		offset += t.file.size + 1
	}

	n, err := t.file.file.ReadAt(b, offset)
	return int64(n), err
}

type ManagedFileRWTxn struct {
	ManagedFileWritableTxn
	ManagedFileReadableTxn
}
