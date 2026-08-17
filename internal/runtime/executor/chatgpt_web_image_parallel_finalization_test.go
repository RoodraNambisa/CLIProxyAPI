package executor

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestFinishChatGPTWebImageUsesConfiguredParallelWholeFinalization(t *testing.T) {
	temporaryDirectory := t.TempDir()
	t.Setenv("TMPDIR", temporaryDirectory)
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 0x44, B: 0xff, A: 0xff})
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/generated/download":
			payload, _ := json.Marshal(map[string]any{"download_url": server.URL + "/asset"})
			response.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = response.Write(payload)
		case "/asset":
			response.Header().Set("Content-Type", "image/png")
			_, _ = response.Write(imageData)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	helps.ConfigureChatGPTWebImageMemoryCapacity(96 << 20)
	cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(4, 4, 4)
	cliproxyexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(4, 2, 2)
	t.Cleanup(func() {
		helps.ConfigureChatGPTWebImageMemoryCapacity(int64(config.DefaultChatGPTWebImageMemoryCapacityMB) << 20)
		cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImageAdmissionQueueSize,
			config.DefaultChatGPTWebImageMaxFinalizers,
		)
		cliproxyexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImagePollConcurrency,
			config.DefaultChatGPTWebImageMemoryFinalizerConcurrency,
		)
	})
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("newRuntimeClient() error = %v", err)
	}
	defer client.CloseIdleConnections()
	leases := helps.NewChatGPTWebImageMemoryLeaseSet()
	t.Cleanup(leases.Release)
	prepared := &chatGPTWebPreparedRequest{
		routeModel:        "gpt-image-2",
		maxImageResults:   1,
		imageMemoryLeases: leases,
		imageConfigSnapshot: cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
			MaxImageResponseBytes: 1 << 20,
		},
		request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{
			Prompt: "draw", OutputFormat: "png",
		}},
	}
	execution := &chatGPTWebImageExecution{response: &fhttp.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n" +
				"data: [DONE]\n\n",
		)),
	}}
	ctx := helps.WithChatGPTWebImageMemoryLeaseSet(t.Context(), leases)
	payload, err := executor.finishChatGPTWebImage(ctx, client, credential, prepared, execution)
	if err != nil {
		t.Fatalf("finishChatGPTWebImage() error = %v", err)
	}
	if !bytes.Contains(payload, []byte(base64.StdEncoding.EncodeToString(imageData))) {
		t.Fatal("finishChatGPTWebImage() omitted encoded image output")
	}
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 1 || snapshot.ProcessingBytes <= 0 || snapshot.FinalizationActive != 0 {
		t.Fatalf("whole finalization retained snapshot = %#v", snapshot)
	}
	if snapshot := cliproxyexecutor.ChatGPTWebImageMemoryFinalizerAdmissionSnapshot(); snapshot.Limit != 2 || snapshot.Active != 0 || snapshot.Admitted == 0 {
		t.Fatalf("memory finalizer admission snapshot = %#v", snapshot)
	}
	entries, errReadDir := os.ReadDir(temporaryDirectory)
	if errReadDir != nil || len(entries) != 0 {
		t.Fatalf("finish completion spool entries = %v, error = %v", entries, errReadDir)
	}
	leases.Release()
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 {
		t.Fatalf("finished whole workspace leaked: %#v", snapshot)
	}
}

func TestDownloadChatGPTWebImagesWithWholeFinalizationSpoolsAndReleasesTwoWorkspaces(t *testing.T) {
	temporaryDirectory := t.TempDir()
	t.Setenv("TMPDIR", temporaryDirectory)
	const capacity = 96 << 20

	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 0xff, G: 0x33, A: 0xff})
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/output/download":
			payload, _ := json.Marshal(map[string]any{"download_url": server.URL + "/asset"})
			response.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = response.Write(payload)
		case "/asset":
			response.Header().Set("Content-Type", "image/png")
			_, _ = response.Write(imageData)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	helps.ConfigureChatGPTWebImageMemoryCapacity(capacity)
	t.Cleanup(func() {
		helps.ConfigureChatGPTWebImageMemoryCapacity(int64(config.DefaultChatGPTWebImageMemoryCapacityMB) << 20)
	})
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("newRuntimeClient() error = %v", err)
	}
	defer client.CloseIdleConnections()
	request := &helps.ChatGPTWebImageRequest{OutputFormat: "png"}
	accumulator := &helps.ChatGPTWebImageAccumulator{References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}}}
	leases := []*helps.ChatGPTWebImageMemoryLeaseSet{
		helps.NewChatGPTWebImageMemoryLeaseSet(),
		helps.NewChatGPTWebImageMemoryLeaseSet(),
	}
	t.Cleanup(func() {
		for _, lease := range leases {
			lease.Release()
		}
	})
	for index, lease := range leases {
		images, finalizationContext, errDownload := executor.downloadChatGPTWebImagesWithWholeFinalization(
			t.Context(), client, credential, accumulator, 1, 1<<20, request, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20}, lease,
		)
		if errDownload != nil {
			t.Fatalf("download %d error = %v, cause = %v", index, errDownload, errors.Unwrap(errDownload))
		}
		if len(images) != 1 || !bytes.Equal(images[0], imageData) || !helps.ChatGPTWebImageWholeFinalizationFromContext(finalizationContext) {
			t.Fatalf("download %d result = images:%d whole:%v", index, len(images), helps.ChatGPTWebImageWholeFinalizationFromContext(finalizationContext))
		}
	}
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 2 || snapshot.ProcessingBytes <= 0 || snapshot.ProcessingBytes > capacity || snapshot.FinalizationActive != 0 {
		t.Fatalf("parallel whole workspaces snapshot = %#v", snapshot)
	}
	entries, errReadDir := os.ReadDir(temporaryDirectory)
	if errReadDir != nil || len(entries) != 0 {
		t.Fatalf("completion spool entries = %v, error = %v", entries, errReadDir)
	}
	for _, lease := range leases {
		lease.Release()
	}
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 || snapshot.WaitingTasks != 0 || snapshot.FinalizationActive != 0 {
		t.Fatalf("released whole workspaces snapshot = %#v", snapshot)
	}
}

func TestDownloadChatGPTWebImagesWithWholeFinalizationRejectsOversizedWorkspaceSafely(t *testing.T) {
	temporaryDirectory := t.TempDir()
	t.Setenv("TMPDIR", temporaryDirectory)

	imageData := chatGPTWebPNGBytes(t, color.NRGBA{B: 0xff, A: 0xff})
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/output/download":
			payload, _ := json.Marshal(map[string]any{"download_url": server.URL + "/asset"})
			response.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			_, _ = response.Write(payload)
		case "/asset":
			response.Header().Set("Content-Type", "image/png")
			_, _ = response.Write(imageData)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	helps.ConfigureChatGPTWebImageMemoryCapacity(chatGPTWebImageResponseJSONOverhead)
	t.Cleanup(func() {
		helps.ConfigureChatGPTWebImageMemoryCapacity(int64(config.DefaultChatGPTWebImageMemoryCapacityMB) << 20)
	})
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("newRuntimeClient() error = %v", err)
	}
	defer client.CloseIdleConnections()
	leases := helps.NewChatGPTWebImageMemoryLeaseSet()
	defer leases.Release()
	_, _, err = executor.downloadChatGPTWebImagesWithWholeFinalization(
		t.Context(), client, credential,
		&helps.ChatGPTWebImageAccumulator{References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}}},
		1, 1<<20, &helps.ChatGPTWebImageRequest{OutputFormat: "png"}, nil,
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20}, leases,
	)
	projected := chatGPTWebWholeFinalizationMemoryError(err)
	var memoryErr *chatGPTWebImageMemoryCapacityError
	if !errors.As(projected, &memoryErr) || memoryErr.ChatGPTWebFailureStage() != "download" || memoryErr.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("oversized whole workspace error = %T %v", projected, projected)
	}
	if snapshot := helps.ChatGPTWebImageMemorySnapshot(); snapshot.ProcessingTasks != 0 || snapshot.ProcessingBytes != 0 || snapshot.WaitingTasks != 0 {
		t.Fatalf("oversized whole workspace changed admission state: %#v", snapshot)
	}
	entries, errReadDir := os.ReadDir(temporaryDirectory)
	if errReadDir != nil || len(entries) != 0 {
		t.Fatalf("oversized completion spool entries = %v, error = %v", entries, errReadDir)
	}
}

func TestEstimateChatGPTWebWholeFinalizationUsesPixelBoundedOutput(t *testing.T) {
	const (
		width             = 1024
		height            = 1024
		inputBytes        = 100_000
		responseByteLimit = 128 << 20
	)
	images := []*chatGPTWebSpooledImage{{
		size:   inputBytes,
		format: "png",
		width:  width,
		height: height,
	}}
	got, err := estimateChatGPTWebWholeFinalizationBytes(
		images,
		&helps.ChatGPTWebImageRequest{OutputFormat: "jpeg"},
		nil,
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{},
		responseByteLimit,
	)
	if err != nil {
		t.Fatalf("estimateChatGPTWebWholeFinalizationBytes() error = %v", err)
	}
	outputBound := chatGPTWebImageEncodedOutputUpperBound(width, height)
	if outputBound >= responseByteLimit {
		t.Fatalf("pixel output bound = %d, want below response cap %d", outputBound, responseByteLimit)
	}
	transient := estimateChatGPTWebImagePostProcessingBytes(width, height, width, height, false, "jpeg")
	base64Bytes := saturatingChatGPTWebImageMultiply(saturatingChatGPTWebImageAdd(outputBound, 2)/3, 4)
	want := saturatingChatGPTWebImageAdd(inputBytes, transient)
	want = saturatingChatGPTWebImageAdd(want, outputBound)
	want = saturatingChatGPTWebImageAdd(want, saturatingChatGPTWebImageMultiply(base64Bytes, 2))
	want = saturatingChatGPTWebImageAdd(want, chatGPTWebImageResponseJSONOverhead)
	if got != want {
		t.Fatalf("whole finalization estimate = %d, want %d", got, want)
	}
}

func TestChatGPTWebImageEncodedOutputUpperBoundCoversSupportedEncoders(t *testing.T) {
	for _, size := range []struct{ width, height int }{{1, 1}, {64, 64}, {257, 129}} {
		source := image.NewNRGBA(image.Rect(0, 0, size.width, size.height))
		var state uint32 = 0x9e3779b9
		for y := 0; y < size.height; y++ {
			for x := 0; x < size.width; x++ {
				state ^= state << 13
				state ^= state >> 17
				state ^= state << 5
				source.SetNRGBA(x, y, color.NRGBA{
					R: uint8(state), G: uint8(state >> 8), B: uint8(state >> 16), A: uint8(state >> 24),
				})
			}
		}
		bound := chatGPTWebImageEncodedOutputUpperBound(size.width, size.height)
		for _, format := range []string{"png", "jpeg", "webp"} {
			encoded, err := encodeChatGPTWebOutputImage(source, format, 100, int(bound))
			if err != nil {
				t.Fatalf("encode %dx%d %s: %v", size.width, size.height, format, err)
			}
			if int64(len(encoded)) > bound {
				t.Fatalf("encoded %dx%d %s bytes = %d, bound = %d", size.width, size.height, format, len(encoded), bound)
			}
		}
	}
}

func TestChatGPTWebWholeFinalizationMemoryErrorsKeepSafeContract(t *testing.T) {
	for _, source := range []error{
		helps.ErrChatGPTWebImageMemoryQueueFull,
		helps.ErrChatGPTWebImageMemoryWorkingSetTooLarge,
	} {
		projected := chatGPTWebWholeFinalizationMemoryError(source)
		var memoryErr *chatGPTWebImageMemoryCapacityError
		if !errors.As(projected, &memoryErr) {
			t.Fatalf("projected %v as %T, want memory capacity error", source, projected)
		}
		if memoryErr.StatusCode() != http.StatusServiceUnavailable || memoryErr.ExecutionResultErrorCode() != "image_memory_capacity" || memoryErr.ChatGPTWebFailureStage() != "download" {
			t.Fatalf("memory contract = status:%d code:%q stage:%q", memoryErr.StatusCode(), memoryErr.ExecutionResultErrorCode(), memoryErr.ChatGPTWebFailureStage())
		}
		if !memoryErr.SkipAuthResult() || memoryErr.RetryOtherAuth() {
			t.Fatalf("memory auth contract = skip:%v retry:%v", memoryErr.SkipAuthResult(), memoryErr.RetryOtherAuth())
		}
	}

	original := errors.New("context-specific failure")
	if projected := chatGPTWebWholeFinalizationMemoryError(original); !errors.Is(projected, original) {
		t.Fatalf("unrelated error = %v, want original", projected)
	}
}
