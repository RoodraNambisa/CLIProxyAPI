package util

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// ReadDecodedRequestBody bounds both wire bytes and each decoded representation.
// Callers retain ownership of closing and replacing the consumed request body.
func ReadDecodedRequestBody(req *http.Request, limit int64) ([]byte, error) {
	if req == nil || limit < 1 || limit == math.MaxInt64 {
		return nil, errors.New("invalid request body limit")
	}
	if req.ContentLength > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	if req.Body == nil {
		return nil, nil
	}
	raw, err := readBoundedRequestBody(req.Body, limit)
	if err != nil {
		return nil, err
	}
	encoding := strings.Join(req.Header.Values("Content-Encoding"), ",")
	if encoding == "" || strings.EqualFold(strings.TrimSpace(encoding), "identity") {
		return raw, nil
	}
	encodings := strings.Split(encoding, ",")
	if len(encodings) > 4 {
		return nil, errors.New("too many request content encodings")
	}
	body := raw
	for i := len(encodings) - 1; i >= 0; i-- {
		switch strings.ToLower(strings.TrimSpace(encodings[i])) {
		case "", "identity":
			continue
		case "zstd":
			if len(body) == 0 {
				return nil, errors.New("empty zstd request body")
			}
			maxMemory := uint64(max(limit, 1<<20))
			decoder, errDecode := zstd.NewReader(bytes.NewReader(body),
				zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(maxMemory))
			if errDecode != nil {
				return nil, fmt.Errorf("invalid zstd request body: %w", errDecode)
			}
			body, errDecode = readBoundedRequestBody(decoder, limit)
			decoder.Close()
			if errDecode != nil {
				if errors.Is(errDecode, zstd.ErrDecoderSizeExceeded) || errors.Is(errDecode, zstd.ErrWindowSizeExceeded) {
					return nil, &http.MaxBytesError{Limit: limit}
				}
				return nil, fmt.Errorf("decode zstd request body: %w", errDecode)
			}
		default:
			return nil, errors.New("unsupported request content encoding")
		}
	}
	req.Header.Del("Content-Encoding")
	req.Header.Del("Content-Length")
	req.Header.Del("Content-MD5")
	req.GetBody = nil
	req.ContentLength = int64(len(body))
	return body, nil
}

func readBoundedRequestBody(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, &http.MaxBytesError{Limit: limit}
	}
	return body, nil
}
