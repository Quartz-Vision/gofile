// Package gofile provides wrappers to manage big numbers of files seamlessly.
//
// It takes care of managing opened and closed files, holding their seek positions, flags etc.
// So you can use ManagedFile as any other file and "open" as many such files as you want.
// Also it provides the transaction API, so you can safely make subsequential calls to the same file across threads.
package gofile

import (
	"io"
	"io/fs"
	"math/rand"
	"os"
	"runtime"
	"sync"
)

const (
	// How many file handles the system can hold simultaneously (applies if GetFilesLimit doesn't work)
	// mandatory: MAX_OPENED_FILES % 4 = 0
	DEFAULT_OPENED_FILES_LIMIT = 32

	// Deduced from tests falling on "failed to create new OS thread" error
	// when there are more than 4096 opened files on 8-threaded machine
	APPROX_MAX_GOROUTINES_PER_THREAD = 512
)

// A ManagedFile wraps a native go file, allowing it to be managed e.g. hold its size, position, state etc.
type ManagedFile struct {
	Path          string
	flag          int
	perm          fs.FileMode
	pos           int64 // current seek position in the file
	size          int64
	file          *os.File
	opened        bool // is the real file opened
	wakingChannel chan bool
	rwmutex       sync.RWMutex
	wakingMutex   sync.Mutex
}

var (
	openedFiles         []*ManagedFile
	globalWakingChannel = make(chan *ManagedFile, 8)
)

// manages some set of file slots for opened files and tries to wake up suspended files if possible
func startWakingManager(fileSlots []*ManagedFile) {
	slotsLen := len(fileSlots)
root:
	for f := range globalWakingChannel {
		for i, tfile := range fileSlots {
			if tfile == nil || !tfile.opened {
				fileSlots[i] = f
				f.opened = true
				f.wakingChannel <- true
				continue root
			}
		}
		for _, i := range rand.Perm(slotsLen) {
			if fileSlots[i].trySuspend() {
				fileSlots[i] = f
				f.opened = true
				f.wakingChannel <- true
				continue root
			}
		}
		f.wakingChannel <- false
	}
}

func init() {
	openedLimitMax := runtime.GOMAXPROCS(0) * APPROX_MAX_GOROUTINES_PER_THREAD / 2
	openedLimit, err := GetFilesLimit()

	if err != nil {
		openedLimit = DEFAULT_OPENED_FILES_LIMIT
	} else if openedLimit > 4 {
		openedLimit = min(uint64(openedLimitMax), openedLimit)
		openedLimit -= openedLimit % 4 // make it odd to avoid problems with the waking manager
	}

	var workersNum uint64
	if openedLimit < 4 {
		workersNum = 1
	} else if openedLimit < 32 {
		workersNum = 2
	} else {
		workersNum = 4 // 4 is optimal to avoid too many context switches
	}

	slotsPerWorker := openedLimit / workersNum
	openedFiles = make([]*ManagedFile, openedLimit)

	for i := uint64(0); i < workersNum; i++ {
		go startWakingManager(openedFiles[i*slotsPerWorker : (i+1)*slotsPerWorker])
	}
}

// Creates a managed file.
//
// Custom flags are unavailable here since the nature of the manager requires the access methods to be as uniform as possible.
func NewFile(filePath string, perm fs.FileMode) (file *ManagedFile, err error) {
	f := &ManagedFile{
		Path:          filePath,
		flag:          os.O_RDWR | os.O_CREATE,
		perm:          perm,
		pos:           0,
		size:          -1,
		file:          nil,
		opened:        false,
		rwmutex:       sync.RWMutex{},
		wakingMutex:   sync.Mutex{},
		wakingChannel: make(chan bool, 1),
	}

	return f, err
}

// Tries to close the file. Returns true if the file is closed eventually
func (f *ManagedFile) trySuspend() bool {
	if !f.rwmutex.TryLock() {
		return false
	}
	if f.opened {
		f.file.Close()
		f.opened = false
	}
	f.rwmutex.Unlock()
	return true
}

// wake waits for a free place in openedFiles and then opens the file
// Should be called ONLY inside the `rwmutex` context
func (f *ManagedFile) wake() (err error) {
	f.wakingMutex.Lock()

	if f.opened {
		f.wakingMutex.Unlock()
		return nil
	}

	for !f.opened { // wait for a free file slot
		globalWakingChannel <- f
		<-f.wakingChannel
	}

	f.file, err = os.OpenFile(f.Path, f.flag, f.perm)
	if err != nil {
		f.opened = false
	} else {
		if f.size == -1 {
			f.size, _ = f.file.Seek(0, io.SeekEnd)
		}
		if f.pos != 0 {
			f.file.Seek(f.pos, io.SeekStart)
		}
	}

	f.wakingMutex.Unlock()
	return err
}

// Trunc truncates given file and sets it's size to 0
// [Thread safe]
func (f *ManagedFile) Trunc() (err error) {
	f.rwmutex.Lock()
	if f.opened {
		f.file.Close()
		f.opened = false
	}

	f.flag |= os.O_TRUNC
	f.size = 0
	f.pos = 0
	err = f.wake()
	f.flag &= ^os.O_TRUNC

	f.rwmutex.Unlock()
	return err
}

// ReadAt reads bytes from a file at given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "read the last byte of the file".
// [Thread safe]
func (f *ManagedFile) ReadAt(b []byte, offset int64) (nRead int64, err error) {
	f.rwmutex.RLock()

	if !f.opened { // don't do the expensive call with mutex
		if err = f.wake(); err != nil {
			f.rwmutex.RUnlock()
			return 0, err
		}
	}

	if offset < 0 {
		offset += f.size + 1
	}

	n, err := f.file.ReadAt(b, offset)

	f.rwmutex.RUnlock()
	return int64(n), err
}

// Read reads bytes from a file at the seek position.
//
// It returns the number of bytes read and the error if there is one.
// [Thread safe]
func (f *ManagedFile) Read(b []byte) (nRead int64, err error) {
	f.rwmutex.RLock()

	if !f.opened { // don't do the expensive call with mutex
		if err = f.wake(); err != nil {
			f.rwmutex.RUnlock()
			return 0, err
		}
	}

	n, err := f.file.Read(b)
	f.pos += int64(n)

	f.rwmutex.RUnlock()
	return int64(n), err
}

// Seek changes current read/write position of the file.
// The offset value is relative to the whence position which is file start, current pos or end (0, 1 or 2 respectively)
// [Thread safe]
func (f *ManagedFile) Seek(offset int64, whence int) (ret int64, err error) {
	f.rwmutex.Lock()

	if (whence == io.SeekStart && offset == f.pos) || (whence == io.SeekEnd && (f.size-offset) == f.pos) || (whence == io.SeekCurrent && offset == 0) {
		pos := f.pos
		f.rwmutex.Unlock()
		return pos, nil
	}

	if !f.opened {
		if err = f.wake(); err != nil {
			f.rwmutex.Unlock()
			return ret, err
		}
	}

	pos, err := f.file.Seek(offset, whence)
	f.pos = pos
	f.rwmutex.Unlock()

	return pos, err
}

// Size returns current size of the file.
// [Thread safe]
func (f *ManagedFile) Size() (length int64, err error) {
	if f.size == -1 {
		err = f.wake()
	}
	return f.size, err
}

// Returns the seek position of the file
// [Thread safe]
func (f *ManagedFile) Position() (pos int64) {
	return f.pos
}

// Write writes the data to a file at the seek position.
//
// It returns the number of bytes written and the error if there is one.
// [Thread safe]
func (f *ManagedFile) Write(b []byte) (nWritten int64, err error) {
	f.rwmutex.Lock()

	if !f.opened {
		if err = f.wake(); err != nil {
			f.rwmutex.Unlock()
			return 0, err
		}
	}

	n, err := f.file.Write(b)
	f.pos += int64(n)
	if f.pos > f.size {
		f.size = f.pos
	}
	f.rwmutex.Unlock()
	return int64(n), err
}

// WriteAt writes bytes to a file at given offset.
// The offset value can be negative to specify offset from the end of the file.
//
// I.e. offset = -2 will mean "write the last byte of the file".
// [Thread safe]
func (f *ManagedFile) WriteAt(b []byte, offset int64) (nWritten int64, err error) {
	f.rwmutex.Lock()

	if !f.opened {
		if err = f.wake(); err != nil {
			f.rwmutex.Unlock()
			return 0, err
		}
	}

	if offset < 0 {
		offset += f.size + 1
	}

	n, err := f.file.WriteAt(b, offset)
	newSize := offset + int64(n)
	if err != nil {
		f.rwmutex.Unlock()
		return 0, err
	}
	if newSize > f.size {
		f.size = newSize
	}

	f.rwmutex.Unlock()
	return int64(n), err
}

// Append appends given data at the end of the file
//
// It returns the position at which the data is written
// [Thread safe]
func (f *ManagedFile) Append(data []byte) (pos int64, err error) {
	f.rwmutex.Lock()

	if !f.opened {
		if err = f.wake(); err != nil {
			f.rwmutex.Unlock()
			return 0, err
		}
	}

	pos = f.size
	n, err := f.file.WriteAt(data, pos)
	f.size += int64(n)

	f.rwmutex.Unlock()
	return pos, err
}

// TRead executes given function m locking the file for writing.
// You can only read from this transaction.
//
// Also the transaction's txn methods are faster than ManagedFile methods
// [Thread safe]
func (f *ManagedFile) TRead(m func(txn Readable) (err error)) (err error) {
	f.rwmutex.RLock()
	if !f.opened {
		if err = f.wake(); err != nil {
			return err
		}
	}
	err = m(&ManagedFileReadableTxn{
		CommonTxn{file: f},
		managedFileReadableTxn{file: f},
	})
	f.rwmutex.RUnlock()

	return err
}

// TWrite executes given function m locking the file for writing from other places.
// You can only write from this transaction.
//
// Also the transaction's txn methods are faster than ManagedFile methods
// [Thread safe]
func (f *ManagedFile) TWrite(m func(txn Writable) (err error)) (err error) {
	f.rwmutex.Lock()
	if !f.opened {
		if err = f.wake(); err != nil {
			return err
		}
	}
	err = m(&ManagedFileWritableTxn{
		CommonTxn{file: f},
		managedFileWritableTxn{file: f},
	})
	f.rwmutex.Unlock()

	return err
}

// TReadWrite executes given function m locking the file for writing/reading from other places.
//
// Also the transaction's txn methods are faster than ManagedFile methods
// [Thread safe]
func (f *ManagedFile) TReadWrite(m func(txn Editable) (err error)) (err error) {
	f.rwmutex.Lock()
	if !f.opened {
		if err = f.wake(); err != nil {
			return err
		}
	}
	err = m(&ManagedFileRWTxn{
		CommonTxn{file: f},
		managedFileWritableTxn{file: f},
		managedFileReadableTxn{file: f},
	})
	f.rwmutex.Unlock()

	return err
}

// PipeTo pipes (copies) the data from start of the file and append it to another (dest) by chunks of size chunkSize.
// [Thread safe]
func (f *ManagedFile) PipeTo(dest Pipeable, chunkSize int64) (err error) {
	if dest == f {
		return f.TReadWrite(func(txn Editable) (err error) {
			size, _ := txn.Size()
			buf := make([]byte, chunkSize)

			for i := int64(0); i < size; i += chunkSize {
				if _, err = txn.ReadAt(buf, i); err != nil {
					return err
				}
				if _, err = txn.Append(buf); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return f.TRead(func(srcTxn Readable) (err error) {
		size, _ := srcTxn.Size()
		buf := make([]byte, chunkSize)

		return dest.TWrite(func(dstTxn Writable) (err error) {
			for i := int64(0); i < size; i += chunkSize {
				if _, err = srcTxn.ReadAt(buf, i); err != nil {
					return err
				}
				if _, err = dstTxn.Append(buf); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// Close closes the file.
// [Thread safe]
func (f *ManagedFile) Close() {
	f.rwmutex.Lock()
	f.wakingMutex.Lock()

	if f.opened {
		f.file.Close()
		f.opened = false
		f.file = nil
	}

	f.wakingMutex.Unlock()
	f.rwmutex.Unlock()
}
