package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type chatGPTWebRemoteImageTestFile struct {
	data         []byte
	contentType  string
	reportedSize int64
	removeCalls  atomic.Int64
}

func (file *chatGPTWebRemoteImageTestFile) WithReader(fn func(io.Reader) error) error {
	if file == nil || fn == nil {
		return errors.New("test file is unavailable")
	}
	return fn(bytes.NewReader(file.data))
}

func (file *chatGPTWebRemoteImageTestFile) Remove() error {
	if file != nil {
		file.removeCalls.Add(1)
	}
	return nil
}

func (file *chatGPTWebRemoteImageTestFile) SizeBytes() int64 {
	if file == nil {
		return 0
	}
	if file.reportedSize > 0 {
		return file.reportedSize
	}
	return int64(len(file.data))
}

func (file *chatGPTWebRemoteImageTestFile) ContentType() string {
	if file == nil {
		return ""
	}
	return file.contentType
}

func TestMaterializeChatGPTWebRemoteImagesDeduplicatesAndRewritesMessages(t *testing.T) {
	remoteURL := "https://images.example:8443/source.png"
	file := &chatGPTWebRemoteImageTestFile{data: chatGPTWebRemoteImagePNG(t, 2, 3), contentType: "image/png"}
	var fetchCalls atomic.Int64
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{
			{ImageURL: remoteURL}, {ImageURL: "  " + remoteURL + "  "},
		}}}},
		originalPayload:   []byte(`{"input":"remote"}`),
		canonicalBody:     []byte(`{"input":"remote"}`),
		imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	prepared.imageConfigSnapshot.RemoteImageURLEnabled = true

	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(_ context.Context, reference, _ string) (chatGPTWebRemoteImageFile, error) {
			fetchCalls.Add(1)
			if reference != remoteURL {
				t.Fatalf("fetch reference = %q", reference)
			}
			return file, nil
		},
	)
	if errMaterialize != nil {
		t.Fatalf("materialize() error = %v", errMaterialize)
	}
	defer prepared.imageMemoryLeases.Release()
	if fetchCalls.Load() != 1 {
		t.Fatalf("fetch calls = %d", fetchCalls.Load())
	}
	for _, part := range prepared.request.Messages[0].Parts {
		if !strings.HasPrefix(part.ImageURL, "data:image/png;base64,") {
			t.Fatalf("rewritten image URL = %q", part.ImageURL)
		}
	}
	if file.removeCalls.Load() != 1 {
		t.Fatalf("remove calls = %d", file.removeCalls.Load())
	}
}

func TestMaterializeChatGPTWebRemoteImagesCompositesRemoteMask(t *testing.T) {
	inputURL := "https://images.example/input.png"
	maskURL := "https://images.example/mask.png"
	files := map[string]*chatGPTWebRemoteImageTestFile{
		inputURL: {data: chatGPTWebRemoteImagePNG(t, 2, 2), contentType: "image/png"},
		maskURL:  {data: chatGPTWebRemoteImageMaskPNG(t, 2, 2), contentType: "image/png"},
	}
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{
			Images: []string{inputURL}, MaskURL: maskURL,
		}},
		imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(_ context.Context, reference, _ string) (chatGPTWebRemoteImageFile, error) {
			return files[reference], nil
		},
	)
	if errMaterialize != nil {
		t.Fatalf("materialize() error = %v", errMaterialize)
	}
	defer prepared.imageMemoryLeases.Release()
	if prepared.request.Image.MaskURL != "" {
		t.Fatalf("mask URL was not consumed: %q", prepared.request.Image.MaskURL)
	}
	if len(prepared.request.Image.Images) != 1 || !strings.HasPrefix(prepared.request.Image.Images[0], "data:image/png;base64,") {
		t.Fatalf("composited image = %#v", prepared.request.Image.Images)
	}
	for reference, file := range files {
		if file.removeCalls.Load() != 1 {
			t.Fatalf("%s remove calls = %d", reference, file.removeCalls.Load())
		}
	}
}

func TestMaterializeChatGPTWebRemoteImagesRejectsMIMEConflictWithoutLeakingLease(t *testing.T) {
	file := &chatGPTWebRemoteImageTestFile{data: chatGPTWebRemoteImagePNG(t, 1, 1), contentType: "image/jpeg"}
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{{
			ImageURL: "https://images.example/image",
		}}}}},
		imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
	before := helps.ChatGPTWebImageMemorySnapshot()
	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) { return file, nil },
	)
	assertChatGPTWebRemoteImageStatus(t, errMaterialize, http.StatusBadRequest, "remote_image_mime_mismatch")
	if !strings.Contains(errMaterialize.Error(), `declared \"image/jpeg\", detected \"image/png\"`) {
		t.Fatalf("remote image mismatch error = %v", errMaterialize)
	}
	prepared.imageMemoryLeases.Release()
	after := helps.ChatGPTWebImageMemorySnapshot()
	if after.ProcessingTasks != before.ProcessingTasks || after.ProcessingBytes != before.ProcessingBytes {
		t.Fatalf("memory snapshot leaked: before=%#v after=%#v", before, after)
	}
	if file.removeCalls.Load() != 1 {
		t.Fatalf("remove calls = %d", file.removeCalls.Load())
	}
}

func TestMaterializeChatGPTWebRemoteImagesMIMENormalizationPolicy(t *testing.T) {
	const remoteURL = "https://images.example/mismatched.jpg"
	tests := []struct {
		name            string
		normalize       bool
		normalizeRemote bool
		wantCode        string
	}{
		{name: "normalization disabled", normalize: false, normalizeRemote: true, wantCode: "remote_image_mime_mismatch"},
		{name: "remote normalization disabled", normalize: true, normalizeRemote: false, wantCode: "remote_image_mime_mismatch"},
		{name: "remote normalization enabled", normalize: true, normalizeRemote: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := &chatGPTWebRemoteImageTestFile{
				data:        chatGPTWebRemoteImagePNG(t, 2, 3),
				contentType: "image/jpeg",
			}
			prepared := &chatGPTWebPreparedRequest{
				request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{{
					ImageURL: remoteURL,
				}}}}},
				imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
			}
			prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
			prepared.imageConfigSnapshot.NormalizeMismatchedImageMIME = tt.normalize
			prepared.imageConfigSnapshot.NormalizeRemoteImageMIME = tt.normalizeRemote

			errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
				t.Context(), prepared, "",
				func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) { return file, nil },
			)
			defer prepared.imageMemoryLeases.Release()
			if tt.wantCode != "" {
				assertChatGPTWebRemoteImageStatus(t, errMaterialize, http.StatusBadRequest, tt.wantCode)
				return
			}
			if errMaterialize != nil {
				t.Fatalf("materialize() error = %v", errMaterialize)
			}
			got := prepared.request.Messages[0].Parts[0].ImageURL
			if !strings.HasPrefix(got, "data:image/png;base64,") {
				t.Fatalf("normalized remote image = %q", got)
			}
		})
	}
}

func TestChatGPTWebRemoteImageDeclaredMIMECompatibility(t *testing.T) {
	tests := []struct {
		declared string
		detected string
		want     bool
	}{
		{declared: "", detected: "image/png", want: true},
		{declared: "application/octet-stream", detected: "image/png", want: true},
		{declared: "image/png; charset=binary", detected: "image/png", want: true},
		{declared: "image/jpg", detected: "image/jpeg", want: true},
		{declared: "text/html", detected: "image/png", want: false},
		{declared: "image/svg+xml", detected: "image/png", want: false},
		{declared: "not a mime", detected: "image/png", want: false},
	}
	for _, testCase := range tests {
		if got := chatGPTWebRemoteImageDeclaredMIMEMatches(testCase.declared, testCase.detected); got != testCase.want {
			t.Fatalf("declared MIME %q detected %q = %v, want %v", testCase.declared, testCase.detected, got, testCase.want)
		}
	}
}

func TestChatGPTWebRemoteImageValidatesAllGIFFramesWithinPixelBudget(t *testing.T) {
	file := &chatGPTWebRemoteImageTestFile{
		data:        chatGPTWebRemoteImageGIF(t, 3, 2, 2),
		contentType: "image/gif",
	}
	metadata, errInspect := inspectChatGPTWebRemoteImage(file, false)
	if errInspect != nil {
		t.Fatalf("inspectChatGPTWebRemoteImage() error = %v", errInspect)
	}
	if metadata.gifFrames != 3 || metadata.decodedPixels != 12 {
		t.Fatalf("GIF metadata = frames:%d pixels:%d", metadata.gifFrames, metadata.decodedPixels)
	}
	if errDecode := validateChatGPTWebRemoteImageDecode(metadata); errDecode != nil {
		t.Fatalf("validateChatGPTWebRemoteImageDecode() error = %v", errDecode)
	}
}

func TestMaterializeChatGPTWebRemoteImagesRejectsNonImagesAndPixelBombs(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		contentType string
	}{
		{name: "HTML", data: []byte("<html><body>not an image</body></html>"), contentType: "text/html"},
		{name: "SVG", data: []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`), contentType: "image/svg+xml"},
		{name: "corrupt PNG", data: []byte("\x89PNG\r\n\x1a\ncorrupt"), contentType: "image/png"},
		{name: "pixel bomb", data: chatGPTWebRemoteImagePNGHeader(100_000, 100_000), contentType: "image/png"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			file := &chatGPTWebRemoteImageTestFile{data: testCase.data, contentType: testCase.contentType}
			prepared := &chatGPTWebPreparedRequest{
				request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{{
					ImageURL: "https://images.example/image",
				}}}}},
				imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
			}
			prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
			errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
				t.Context(), prepared, "",
				func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) { return file, nil },
			)
			assertChatGPTWebRemoteImageStatus(t, errMaterialize, http.StatusBadRequest, "remote_image_invalid_content")
			prepared.imageMemoryLeases.Release()
			if file.removeCalls.Load() != 1 {
				t.Fatalf("remove calls = %d", file.removeCalls.Load())
			}
		})
	}
}

func TestMaterializeChatGPTWebRemoteImagesClassifiesFetchFailures(t *testing.T) {
	tests := []struct {
		name       string
		fetchError error
		status     int
		code       string
	}{
		{name: "invalid URL", fetchError: &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageInvalid}, status: 400, code: "remote_image_url_invalid"},
		{name: "blocked URL", fetchError: &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageBlocked}, status: 400, code: "remote_image_url_blocked"},
		{name: "too large", fetchError: &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageTooLarge}, status: 413, code: "remote_image_too_large"},
		{name: "network", fetchError: &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageFetch}, status: 502, code: "remote_image_fetch_failed"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			prepared := &chatGPTWebPreparedRequest{
				request:           helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{{ImageURL: "https://images.example/image"}}}}},
				imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
			}
			prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
			errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
				t.Context(), prepared, "",
				func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) {
					return nil, testCase.fetchError
				},
			)
			assertChatGPTWebRemoteImageStatus(t, errMaterialize, testCase.status, testCase.code)
			prepared.imageMemoryLeases.Release()
		})
	}
}

func TestMaterializeChatGPTWebRemoteImagesHonorsDisabledSnapshotWithoutFetching(t *testing.T) {
	var fetchCalls atomic.Int64
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{{ImageURL: "https://images.example/image"}}}}},
	}
	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) {
			fetchCalls.Add(1)
			return nil, errors.New("unexpected fetch")
		},
	)
	if errMaterialize != nil || fetchCalls.Load() != 0 {
		t.Fatalf("disabled materialize = error:%v fetches:%d", errMaterialize, fetchCalls.Load())
	}
}

func TestMaterializeChatGPTWebRemoteImagesDownloadsSequentially(t *testing.T) {
	urls := []string{"https://images.example/one.png", "https://images.example/two.png"}
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{
			{ImageURL: urls[0]}, {ImageURL: urls[1]},
		}}}},
		imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
	var mu sync.Mutex
	active := 0
	peak := 0
	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) {
			mu.Lock()
			active++
			peak = max(peak, active)
			active--
			mu.Unlock()
			return &chatGPTWebRemoteImageTestFile{data: chatGPTWebRemoteImagePNG(t, 1, 1), contentType: "image/png"}, nil
		},
	)
	if errMaterialize != nil {
		t.Fatalf("materialize() error = %v", errMaterialize)
	}
	prepared.imageMemoryLeases.Release()
	if peak != 1 {
		t.Fatalf("fetch peak = %d", peak)
	}
}

func TestChatGPTWebRemoteImageProxyModeUsesOnlyCredentialEffectiveProxy(t *testing.T) {
	auth := &cliproxyauth.Auth{
		ProxyURL:        "http://credential-proxy.example:8080",
		RuntimeProxyURL: "http://pool-proxy.example:8080",
	}
	prepared := &chatGPTWebPreparedRequest{}
	prepared.imageConfigSnapshot.RemoteImageURLDownloadMode = config.ChatGPTWebRemoteImageDownloadDirect
	if proxyURL := chatGPTWebRemoteImageProxyURL(auth, prepared); proxyURL != "" {
		t.Fatalf("direct mode proxy = %q", proxyURL)
	}

	prepared.imageConfigSnapshot.RemoteImageURLDownloadMode = config.ChatGPTWebRemoteImageDownloadCredentialProxy
	if proxyURL := chatGPTWebRemoteImageProxyURL(auth, prepared); proxyURL != auth.ProxyURL {
		t.Fatalf("credential mode proxy = %q, want %q", proxyURL, auth.ProxyURL)
	}
	auth.ProxyURL = ""
	if proxyURL := chatGPTWebRemoteImageProxyURL(auth, prepared); proxyURL != auth.RuntimeProxyURL {
		t.Fatalf("runtime pool proxy = %q, want %q", proxyURL, auth.RuntimeProxyURL)
	}
	auth.RuntimeProxyURL = ""
	if proxyURL := chatGPTWebRemoteImageProxyURL(auth, prepared); proxyURL != "" {
		t.Fatalf("missing credential proxy fallback = %q", proxyURL)
	}
	auth.ProxyURL = "direct"
	if proxyURL := chatGPTWebRemoteImageProxyURL(auth, prepared); proxyURL != "" {
		t.Fatalf("explicit direct credential proxy = %q", proxyURL)
	}
}

func TestMaterializeChatGPTWebRemoteImagesEnforcesAggregateSizeBeforeMemoryAdmission(t *testing.T) {
	urls := []string{"https://images.example/one.png", "https://images.example/two.png", "https://images.example/three.png"}
	files := map[string]*chatGPTWebRemoteImageTestFile{
		urls[0]: {data: chatGPTWebRemoteImagePNG(t, 1, 1), contentType: "image/png", reportedSize: 50 << 20},
		urls[1]: {data: chatGPTWebRemoteImagePNG(t, 1, 1), contentType: "image/png", reportedSize: 50 << 20},
		urls[2]: {data: chatGPTWebRemoteImagePNG(t, 1, 1), contentType: "image/png", reportedSize: 1},
	}
	prepared := &chatGPTWebPreparedRequest{
		request: helps.ChatGPTWebRequest{Messages: []helps.ChatGPTWebMessage{{Parts: []helps.ChatGPTWebContentPart{
			{ImageURL: urls[0]}, {ImageURL: urls[1]}, {ImageURL: urls[2]},
		}}}},
		imageMemoryLeases: helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	prepared.imageConfigSnapshot.RemoteImageURLEnabled = true
	errMaterialize := (&ChatGPTWebExecutor{}).materializeChatGPTWebRemoteImagesWithFetcher(
		t.Context(), prepared, "",
		func(_ context.Context, reference, _ string) (chatGPTWebRemoteImageFile, error) {
			return files[reference], nil
		},
	)
	assertChatGPTWebRemoteImageStatus(t, errMaterialize, http.StatusRequestEntityTooLarge, "remote_image_too_large")
	prepared.imageMemoryLeases.Release()
	for reference, file := range files {
		if file.removeCalls.Load() != 1 {
			t.Fatalf("%s remove calls = %d", reference, file.removeCalls.Load())
		}
	}
}

func TestChatGPTWebRemoteImageFetchFailurePrecedesUpstreamAndRequestSlotCommit(t *testing.T) {
	var upstreamCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	t.Cleanup(server.Close)

	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{Images: config.ImagesConfig{
		ChatGPTWeb: config.ChatGPTWebImageConfig{RemoteImageURLEnabled: true},
	}}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL
	var fetchCalls atomic.Int64
	executor.remoteImageFetcher = func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) {
		fetchCalls.Add(1)
		return nil, &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageBlocked}
	}
	diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
	reservation := &chatGPTWebUsageTestReservation{reserved: true}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.SetDiagnostics(diagnostics)
	slot.Bind(reservation)
	_, errExecute := executor.Execute(t.Context(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":"https://images.example/input.png"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse,
		AuthRequestSlot: slot, ExecutionDiagnostics: diagnostics,
	})
	assertChatGPTWebRemoteImageStatus(t, errExecute, http.StatusBadRequest, "remote_image_url_blocked")
	if fetchCalls.Load() != 1 || upstreamCalls.Load() != 0 {
		t.Fatalf("fetch/upstream calls = %d/%d", fetchCalls.Load(), upstreamCalls.Load())
	}
	if reservation.committed || !reservation.reserved || slot.Committed() {
		t.Fatalf("request slot after local failure = reserved:%v committed:%v", reservation.reserved, reservation.committed)
	}
	snapshot := diagnostics.Snapshot()
	if snapshot.FailureStage != "input_download" || snapshot.ErrorCode != "remote_image_url_blocked" ||
		snapshot.UpstreamCommitted || snapshot.AuthRequestSlotConsumed {
		t.Fatalf("diagnostics = %#v", snapshot)
	}
	if !slot.Release() {
		t.Fatal("AuthRequestSlot.Release() = false")
	}
}

func TestChatGPTWebRemoteImageStreamFetchFailurePrecedesUpstreamAndRequestSlotCommit(t *testing.T) {
	var upstreamCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	t.Cleanup(server.Close)

	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{Images: config.ImagesConfig{
		ChatGPTWeb: config.ChatGPTWebImageConfig{RemoteImageURLEnabled: true},
	}}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL
	var fetchCalls atomic.Int64
	executor.remoteImageFetcher = func(context.Context, string, string) (chatGPTWebRemoteImageFile, error) {
		fetchCalls.Add(1)
		return nil, &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageBlocked}
	}
	diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
	reservation := &chatGPTWebUsageTestReservation{reserved: true}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.SetDiagnostics(diagnostics)
	slot.Bind(reservation)
	stream, errExecute := executor.ExecuteStream(t.Context(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://images.example/input.png"}]}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse,
		Stream: true, AuthRequestSlot: slot, ExecutionDiagnostics: diagnostics,
	})
	if stream != nil {
		t.Fatal("stream result is not nil")
	}
	assertChatGPTWebRemoteImageStatus(t, errExecute, http.StatusBadRequest, "remote_image_url_blocked")
	if fetchCalls.Load() != 1 || upstreamCalls.Load() != 0 || reservation.committed || slot.Committed() {
		t.Fatalf("stream local failure = fetch:%d upstream:%d committed:%v/%v", fetchCalls.Load(), upstreamCalls.Load(), reservation.committed, slot.Committed())
	}
	snapshot := diagnostics.Snapshot()
	if snapshot.FailureStage != "input_download" || snapshot.ErrorCode != "remote_image_url_blocked" ||
		snapshot.UpstreamCommitted || snapshot.AuthRequestSlotConsumed {
		t.Fatalf("stream diagnostics = %#v", snapshot)
	}
	if !slot.Release() {
		t.Fatal("AuthRequestSlot.Release() = false")
	}
}

func TestChatGPTWebRemoteImageConfigSnapshotSurvivesHotDisableForStartedRequest(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{Images: config.ImagesConfig{
		ChatGPTWeb: config.ChatGPTWebImageConfig{
			RemoteImageURLEnabled:      true,
			RemoteImageURLDownloadMode: config.ChatGPTWebRemoteImageDownloadCredentialProxy,
		},
	}}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	request := cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":[{"role":"user","content":[{"type":"input_image","image_url":"https://images.example/input.png"}]}]}`),
	}
	options := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse}
	template, errPrepare := executor.prepareRuntimeRequestTemplate(t.Context(), request, options, false)
	if errPrepare != nil {
		t.Fatalf("prepare enabled request: %v", errPrepare)
	}
	executor.UpdateConfig(&config.Config{})
	options = cliproxyexecutor.WithProviderPreparedRequest(options, executor.Identifier(), template)
	var fetchCalls atomic.Int64
	var receivedProxy atomic.Value
	executor.remoteImageFetcher = func(_ context.Context, _, proxyURL string) (chatGPTWebRemoteImageFile, error) {
		fetchCalls.Add(1)
		receivedProxy.Store(proxyURL)
		return nil, &helps.ChatGPTWebRemoteImageError{Kind: helps.ChatGPTWebRemoteImageBlocked}
	}
	auth := chatGPTWebRuntimeAuth()
	auth.RuntimeProxyURL = "http://pool-proxy.example:8080"
	_, errExecute := executor.Execute(t.Context(), auth, request, options)
	assertChatGPTWebRemoteImageStatus(t, errExecute, http.StatusBadRequest, "remote_image_url_blocked")
	if fetchCalls.Load() != 1 {
		t.Fatalf("started request fetch calls = %d", fetchCalls.Load())
	}
	if receivedProxy.Load() != auth.RuntimeProxyURL {
		t.Fatalf("started request proxy = %v, want %q", receivedProxy.Load(), auth.RuntimeProxyURL)
	}
	if _, errNew := executor.prepareRuntimeRequestTemplate(t.Context(), request, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse,
	}, false); errNew == nil || !strings.Contains(errNew.Error(), "only supports base64") {
		t.Fatalf("new disabled request error = %v", errNew)
	}
}

func TestChatGPTWebRemoteImageInputsTranslateFromChatAndResponses(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{Images: config.ImagesConfig{
		ChatGPTWeb: config.ChatGPTWebImageConfig{RemoteImageURLEnabled: true},
	}}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	tests := []struct {
		name   string
		format sdktranslator.Format
		body   string
	}{
		{
			name:   "chat completions",
			format: sdktranslator.FormatOpenAI,
			body:   `{"model":"gpt-5","messages":[{"role":"user","content":[{"type":"text","text":"describe"},{"type":"image_url","image_url":{"url":"https://images.example/chat.png"}}]}]}`,
		},
		{
			name:   "responses",
			format: sdktranslator.FormatOpenAIResponse,
			body:   `{"model":"gpt-5","input":[{"role":"user","content":[{"type":"input_text","text":"describe"},{"type":"input_image","image_url":"https://images.example/responses.png"}]}]}`,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			prepared, errPrepare := executor.prepareRuntimeRequestTemplate(t.Context(), cliproxyexecutor.Request{
				Model: "gpt-5", Payload: []byte(testCase.body),
			}, cliproxyexecutor.Options{SourceFormat: testCase.format, ResponseFormat: testCase.format}, false)
			if errPrepare != nil {
				t.Fatalf("prepareRuntimeRequestTemplate() error = %v", errPrepare)
			}
			if !chatGPTWebPreparedRequestHasRemoteImages(prepared) {
				t.Fatalf("prepared request lost remote image: %#v", prepared.request.Messages)
			}
		})
	}
}

func assertChatGPTWebRemoteImageStatus(t *testing.T, err error, wantStatus int, wantCode string) {
	t.Helper()
	var status interface{ StatusCode() int }
	var code interface{ ExecutionResultErrorCode() string }
	var stage interface{ ChatGPTWebFailureStage() string }
	var skip interface{ SkipAuthResult() bool }
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &status) || status.StatusCode() != wantStatus ||
		!errors.As(err, &code) || code.ExecutionResultErrorCode() != wantCode ||
		!errors.As(err, &stage) || stage.ChatGPTWebFailureStage() != "input_download" ||
		!errors.As(err, &skip) || !skip.SkipAuthResult() ||
		!errors.As(err, &retry) || retry.RetryOtherAuth() {
		t.Fatalf("remote image error contract = %v", err)
	}
}

func chatGPTWebRemoteImagePNG(t *testing.T, width, height int) []byte {
	t.Helper()
	canvas := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			canvas.Set(x, y, color.NRGBA{R: 30, G: 90, B: 180, A: 255})
		}
	}
	var output bytes.Buffer
	if errEncode := png.Encode(&output, canvas); errEncode != nil {
		t.Fatal(errEncode)
	}
	return output.Bytes()
}

func chatGPTWebRemoteImageMaskPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	canvas := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			canvas.Set(x, y, color.NRGBA{A: 255})
		}
	}
	var output bytes.Buffer
	if errEncode := png.Encode(&output, canvas); errEncode != nil {
		t.Fatal(errEncode)
	}
	return output.Bytes()
}

func chatGPTWebRemoteImageGIF(t *testing.T, frames, width, height int) []byte {
	t.Helper()
	animation := &gif.GIF{LoopCount: 0, Config: image.Config{
		ColorModel: color.Palette{color.Black, color.White},
		Width:      width,
		Height:     height,
	}}
	for index := 0; index < frames; index++ {
		frame := image.NewPaletted(image.Rect(0, 0, width, height), color.Palette{color.Black, color.White})
		for pixel := range frame.Pix {
			frame.Pix[pixel] = uint8(index % 2)
		}
		animation.Image = append(animation.Image, frame)
		animation.Delay = append(animation.Delay, 1)
	}
	var output bytes.Buffer
	if errEncode := gif.EncodeAll(&output, animation); errEncode != nil {
		t.Fatal(errEncode)
	}
	return output.Bytes()
}

func chatGPTWebRemoteImagePNGHeader(width, height uint32) []byte {
	var output bytes.Buffer
	output.WriteString("\x89PNG\r\n\x1a\n")
	data := make([]byte, 13)
	binary.BigEndian.PutUint32(data[0:4], width)
	binary.BigEndian.PutUint32(data[4:8], height)
	data[8] = 8
	data[9] = 6
	binary.Write(&output, binary.BigEndian, uint32(len(data)))
	output.WriteString("IHDR")
	output.Write(data)
	checksum := crc32.NewIEEE()
	_, _ = checksum.Write([]byte("IHDR"))
	_, _ = checksum.Write(data)
	binary.Write(&output, binary.BigEndian, checksum.Sum32())
	return output.Bytes()
}
