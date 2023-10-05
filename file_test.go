package gofile

import (
	"io"
	"path"
	"sync"
	"testing"

	"github.com/Quartz-Vision/goslice"
)

func TestBasicFileCRU(t *testing.T) {
	filePath := path.Join(t.TempDir(), "CRUfile.tmp")
	testContents := []byte{'a', 'b', 'c', 'd', 'e'}

	file, err := NewFile(filePath, 0o777)
	if err != nil {
		t.Errorf("File: Create: %s", err.Error())
		return
	}

	nWritten, err := file.Write(testContents)
	if err != nil {
		t.Errorf("File: Write: %s", err.Error())
		return
	} else if nWritten != int64(len(testContents)) {
		t.Errorf("File: Write: nWritten != content length")
		return
	}

	pos, err := file.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("File: Seek: %s", err.Error())
		return
	} else if pos != 0 {
		t.Errorf("File: Seek: new position != 0")
		return
	}

	readContents := make([]byte, len(testContents))
	_, err = file.Read(readContents)
	if err != nil && err != io.EOF {
		t.Errorf("File: Read: %s", err.Error())
		return
	} else if !goslice.Equal(readContents, testContents) {
		t.Errorf("File: Read: read contents != test contents")
		return
	}
}

func TestBurstFileCRU(t *testing.T) {
	testContents := []byte{'a', 'b', 'c', 'd', 'e'}
	wg := sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(w *sync.WaitGroup) {
			filePath := path.Join(t.TempDir(), "BurstCRUfile.tmp")

			for i := 0; i < 100; i++ {
				file, err := NewFile(filePath, 0o777)
				if err != nil {
					t.Errorf("File: Create: %s", err.Error())
					return
				}

				err = file.Trunc()
				if err != nil {
					t.Errorf("File: Trunc: %s", err.Error())
					return
				}

				nWritten, err := file.Write(testContents)
				if err != nil {
					t.Errorf("File: Write: %s", err.Error())
					return
				} else if nWritten != int64(len(testContents)) {
					t.Errorf("File: Write: nWritten != content length")
					return
				}

				pos, err := file.Seek(0, io.SeekStart)
				if err != nil {
					t.Errorf("File: Seek: %s", err.Error())
					return
				} else if pos != 0 {
					t.Errorf("File: Seek: new position != 0")
					return
				}

				readContents := make([]byte, len(testContents))
				_, err = file.Read(readContents)
				if err != nil && err != io.EOF {
					t.Errorf("File: Read: %s", err.Error())
					return
				} else if !goslice.Equal(readContents, testContents) {
					t.Errorf("File: Read: read contents != test contents")
					return
				}

				file.Close()
			}
			w.Done()
		}(&wg)
	}
	wg.Wait()
}
