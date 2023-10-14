package gofile

import (
	"fmt"
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
	}
	if !goslice.Equal(readContents, testContents) {
		t.Errorf("File: Read: read contents != test contents")
		return
	}

	t.Log("Testing ReadAt after Append")

	readContents = make([]byte, len(testContents))
	file.Append(testContents)
	_, err = file.ReadAt(readContents, int64(len(testContents)))
	if err != nil && err != io.EOF {
		t.Errorf("File: Read: %s", err.Error())
		return
	}
	if !goslice.Equal(readContents, testContents) {
		t.Errorf("File: Read: read contents != test contents")
		return
	}
}

func TestBurstFileCRU(t *testing.T) {
	const testFilesNum = 20000
	var err error
	tmpDir := t.TempDir()
	wg := sync.WaitGroup{}
	testContents := []byte{'a', 'b', 'c', 'd', 'e'}

	files := [testFilesNum]*ManagedFile{}
	for i := 0; i < testFilesNum; i++ {
		files[i], err = NewFile(path.Join(tmpDir, fmt.Sprintf("test%d.tmp", i)), 0o777)
		if err != nil {
			t.Errorf("File: Create(%d): %s", i, err.Error())
			return
		}
	}

	testFile := func(w *sync.WaitGroup, i int) {
		file := files[i]

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

		w.Done()
	}

	for i := 0; i < testFilesNum; i++ {
		wg.Add(1)
		go testFile(&wg, i)
	}
	wg.Wait()

	for i := 0; i < testFilesNum; i++ {
		files[i].Close()
	}
}
