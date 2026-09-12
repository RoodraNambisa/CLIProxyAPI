package managementasset

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	log "github.com/sirupsen/logrus"
)

// FileServer keeps at most one bounded, immutable panel snapshot and its encodings.
// Its zero value is ready for use. No lock is held while writing to a client.
type FileServer struct {
	mu    sync.Mutex
	asset *servedAsset
}

type servedAsset struct {
	path    string
	info    os.FileInfo
	data    []byte
	etag    string
	encoded map[string][]byte
}

// ServeFile serves only the panel asset; compression never wraps API or SSE responses.
func (s *FileServer) ServeFile(w http.ResponseWriter, r *http.Request, path string) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			http.NotFound(w, r)
		} else {
			http.Error(w, "Unable to read management panel", http.StatusInternalServerError)
		}
		return
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			log.WithError(errClose).Warn("failed to close management panel asset")
		}
	}()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.NotFound(w, r)
		return
	}

	w.Header().Add("Vary", "Accept-Encoding")
	// Store the file in the browser, but validate it on every navigation/update.
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	encoding, acceptable := panelEncoding(r.Header, info.Size() <= maxAssetDownloadSize)
	if !acceptable {
		http.Error(w, "No acceptable management panel encoding", http.StatusNotAcceptable)
		return
	}
	asset, data, err := s.snapshot(f, path, info, encoding)
	if err != nil {
		log.WithError(err).Warn("failed to prepare management panel asset")
		http.Error(w, "Unable to read management panel", http.StatusInternalServerError)
		return
	}
	if asset == nil {
		// Preserve support for oversized, manually installed panels without caching them.
		http.ServeContent(w, r, ManagementFileName, info.ModTime(), f)
		return
	}
	etag := asset.etag
	if encoding != "" {
		w.Header().Set("Content-Encoding", encoding)
		etag += "-" + encoding
		if r.Header.Get("Range") != "" {
			// A client forbidding identity receives the full encoded representation.
			// Multipart byte ranges must not be labeled as one compressed stream.
			r = r.Clone(r.Context())
			r.Header.Del("Range")
			r.Header.Del("If-Range")
		}
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	// ServeContent omits this for encoded content unless it is supplied explicitly.
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	http.ServeContent(w, r, ManagementFileName, asset.info.ModTime(), bytes.NewReader(data))
}

func samePanelFile(a, b os.FileInfo) bool {
	return os.SameFile(a, b) && a.Size() == b.Size() && a.ModTime().Equal(b.ModTime())
}

func (s *FileServer) snapshot(f *os.File, path string, info os.FileInfo, encoding string) (*servedAsset, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info.Size() > maxAssetDownloadSize {
		s.asset = nil
		return nil, nil, nil
	}
	asset := s.asset
	if asset == nil || asset.path != path || !samePanelFile(asset.info, info) {
		data, err := io.ReadAll(io.LimitReader(f, maxAssetDownloadSize+1))
		if err != nil {
			return nil, nil, err
		}
		after, err := f.Stat()
		if err != nil {
			return nil, nil, err
		}
		if len(data) > maxAssetDownloadSize || int64(len(data)) != info.Size() || !samePanelFile(info, after) {
			return nil, nil, fmt.Errorf("management panel changed while reading")
		}
		asset = &servedAsset{
			path: path, info: info, data: data,
			etag: fmt.Sprintf("%x", sha256.Sum256(data)), encoded: make(map[string][]byte),
		}
		s.asset = asset
	}
	if encoding == "" {
		return asset, asset.data, nil
	}
	if data, ok := asset.encoded[encoding]; ok {
		return asset, data, nil
	}
	var output bytes.Buffer
	var writer io.WriteCloser
	if encoding == "br" {
		writer = brotli.NewWriterLevel(&output, 4)
	} else {
		writer = gzip.NewWriter(&output)
	}
	_, errWrite := writer.Write(asset.data)
	errClose := writer.Close()
	if errWrite != nil {
		return nil, nil, errWrite
	}
	if errClose != nil {
		return nil, nil, errClose
	}
	data := output.Bytes()
	asset.encoded[encoding] = data
	return asset, data, nil
}

func panelEncoding(header http.Header, compressible bool) (string, bool) {
	qualities := make(map[string]float64)
	for _, entry := range strings.Split(strings.Join(header.Values("Accept-Encoding"), ","), ",") {
		parts := strings.Split(entry, ";")
		name := strings.ToLower(strings.TrimSpace(parts[0]))
		if name == "" {
			continue
		}
		q := 1.0
		for _, param := range parts[1:] {
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
				q = 0
				break
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil || !(parsed >= 0 && parsed <= 1) {
				q = 0
				break
			}
			q = parsed
		}
		qualities[name] = q
	}
	identity, explicitIdentity := qualities["identity"]
	if !explicitIdentity {
		identity = 1
		if star, exists := qualities["*"]; exists && star == 0 {
			identity = 0
		}
	}
	// Keep existing byte-range semantics on the unencoded HTML whenever acceptable.
	if r := header.Get("Range"); r != "" && identity > 0 {
		return "", true
	}
	selected, best := "", 0.0
	if compressible {
		for _, encoding := range []string{"br", "gzip"} {
			q, exists := qualities[encoding]
			if !exists {
				q = qualities["*"]
			}
			if q > best {
				selected, best = encoding, q
			}
		}
	}
	if (explicitIdentity && identity > best) || best == 0 {
		return "", identity > 0
	}
	return selected, true
}
