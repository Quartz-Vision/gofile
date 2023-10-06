package gofile

import (
	"io"
	"path"
	"testing"

	"github.com/Quartz-Vision/goslice"
)

func TestBasicBufferedCRU(t *testing.T) {
	testContents := []byte{'a', 'b', 'c', 'd', 'e'}
	var file File
	var err error

	// layers = 1 means wi will be wrapping every buffer with the next one
	// e.g. NewBufferedFile(NewBufferedFile(file, 1), 2), 3), ...)
	for layers := 0; layers < 2; layers++ {
		if layers != 0 {
			filePath := path.Join(t.TempDir(), "CRUfile.tmp")
			file, err = NewFile(filePath, 0o777)
			if err != nil {
				t.Errorf("File: Create: %s", err.Error())
				return
			}
		}
		for i := int64(1); i < 7; i++ {
			if layers == 0 {
				filePath := path.Join(t.TempDir(), "CRUfile.tmp")
				file, err = NewFile(filePath, 0o777)
				if err != nil {
					t.Errorf("File: Create: %s", err.Error())
					return
				}
			}
			t.Logf("Contents size: %d; Buffer size: %d; Buffer layers: %d", len(testContents), i, layers)

			file, err = NewBufferedFile(file, i)
			if err != nil {
				t.Errorf("File: Create Buffer: %s", err.Error())
				return
			}

			nWritten, err := file.Write(testContents)
			if err != nil {
				t.Errorf("Buffered file: Write: %s", err.Error())
				return
			} else if nWritten != int64(len(testContents)) {
				t.Errorf("Buffered file: Write: nWritten != content length")
				return
			}

			pos, err := file.Seek(0, io.SeekStart)
			if err != nil {
				t.Errorf("Buffered file: Seek: %s", err.Error())
				return
			} else if pos != 0 {
				t.Errorf("Buffered file: Seek: new position != 0")
				return
			}

			readContents := make([]byte, len(testContents))
			_, err = file.Read(readContents)
			if err != nil && err != io.EOF {
				t.Errorf("Buffered file: Read 1: %s", err.Error())
				return
			} else if !goslice.Equal(readContents, testContents) {
				t.Errorf("Buffered file: Read 1: read contents != test contents")
				return
			}

			file.Seek(0, io.SeekStart)
			readContents = make([]byte, len(testContents))
			_, err = file.Read(readContents)
			if err != nil && err != io.EOF {
				t.Errorf("Buffered file: Read 2: %s", err.Error())
				return
			} else if !goslice.Equal(readContents, testContents) {
				t.Errorf("Buffered file: Read 2: read contents != test contents")
				return
			}
		}
	}
}
