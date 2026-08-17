package executor

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestReadChatGPTWebBoundedBodyWithAdmissionSpoolsUnknownLengthAndCleansUp(t *testing.T) {
	temporaryDirectory := t.TempDir()
	t.Setenv("TMPDIR", temporaryDirectory)
	payload := bytes.Repeat([]byte("image"), 1024)
	spoolBefore := helps.ChatGPTWebImageSpoolSnapshot()
	var acquiredBytes atomic.Int64
	var releases atomic.Int32

	got, err := readChatGPTWebBoundedBodyWithAdmission(bytes.NewReader(payload), -1, len(payload), func(size int64) (func(), error) {
		acquiredBytes.Store(size)
		return func() { releases.Add(1) }, nil
	})
	if err != nil {
		t.Fatalf("read unknown-length body: %v", err)
	}
	if !bytes.Equal(got, payload) || acquiredBytes.Load() != int64(len(payload)) || releases.Load() != 1 {
		t.Fatalf("body lifecycle = bytes:%d acquired:%d releases:%d", len(got), acquiredBytes.Load(), releases.Load())
	}
	files, errGlob := filepath.Glob(filepath.Join(temporaryDirectory, "cliproxy-chatgpt-web-image-*"))
	if errGlob != nil {
		t.Fatalf("glob temporary image files: %v", errGlob)
	}
	if len(files) != 0 {
		t.Fatalf("temporary image files remain: %v", files)
	}
	if entries, errReadDir := os.ReadDir(temporaryDirectory); errReadDir != nil || len(entries) != 0 {
		t.Fatalf("temporary directory entries = %v, error = %v", entries, errReadDir)
	}
	spoolAfter := helps.ChatGPTWebImageSpoolSnapshot()
	if spoolAfter.CurrentFiles != spoolBefore.CurrentFiles || spoolAfter.CurrentBytes != spoolBefore.CurrentBytes ||
		spoolAfter.CreatedFiles != spoolBefore.CreatedFiles+1 || spoolAfter.CleanedFiles != spoolBefore.CleanedFiles+1 {
		t.Fatalf("spool metrics before=%#v after=%#v", spoolBefore, spoolAfter)
	}
}

func TestReadChatGPTWebBoundedBodyWithAdmissionCleansSpoolOnReadFailureAndOverflow(t *testing.T) {
	tests := []struct {
		name   string
		reader io.Reader
		limit  int
	}{
		{name: "read failure", reader: errorImageReader{}, limit: 8},
		{name: "overflow", reader: bytes.NewReader([]byte("123456789")), limit: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := helps.ChatGPTWebImageSpoolSnapshot()
			if _, err := readChatGPTWebBoundedBodyWithAdmission(test.reader, -1, test.limit, func(int64) (func(), error) {
				return func() {}, nil
			}); err == nil {
				t.Fatal("read error = nil")
			}
			after := helps.ChatGPTWebImageSpoolSnapshot()
			if after.CurrentFiles != before.CurrentFiles || after.CurrentBytes != before.CurrentBytes ||
				after.CreatedFiles != before.CreatedFiles+1 || after.CleanedFiles != before.CleanedFiles+1 {
				t.Fatalf("spool metrics before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestReadChatGPTWebBoundedBodyWithAdmissionRejectsBeforeKnownLengthRead(t *testing.T) {
	reader := &countingImageReader{Reader: bytes.NewReader([]byte("image"))}
	_, err := readChatGPTWebBoundedBodyWithAdmission(reader, 5, 10, func(int64) (func(), error) {
		return nil, &chatGPTWebImageMemoryCapacityError{}
	})
	if err == nil {
		t.Fatal("capacity error = nil")
	}
	if reader.reads.Load() != 0 {
		t.Fatalf("body reads = %d, want zero before admission", reader.reads.Load())
	}
}

type countingImageReader struct {
	*bytes.Reader
	reads atomic.Int32
}

type errorImageReader struct{}

func (errorImageReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func (reader *countingImageReader) Read(buffer []byte) (int, error) {
	reader.reads.Add(1)
	return reader.Reader.Read(buffer)
}
