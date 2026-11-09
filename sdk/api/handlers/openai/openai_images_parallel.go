package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
)

// Mixed-provider and native routes keep their existing aggregation semantics.
func (h *OpenAIImagesAPIHandler) webImageAggregationConcurrency(c *gin.Context, providers []string, count int, snapshot core.ChatGPTWebImageConfigSnapshot) int {
	if count < 2 || snapshot.NAggregationConcurrency < 2 || h.AuthManager == nil || nativeImageRequest(c) != nil || !imageProvidersContain(providers, ChatGPTWeb) {
		return 1
	}
	if !chatGPTWebAllowedWithoutCodex(c) {
		for _, provider := range providers {
			if provider != ChatGPTWeb {
				if _, exists := h.AuthManager.Executor(provider); exists {
					return 1
				}
			}
		}
	}
	return min(count, snapshot.NAggregationConcurrency)
}

type parallelWebImageEvent struct {
	index        int
	ctx          context.Context
	chunk        []byte
	headers      http.Header
	path         string
	size         int64
	binaryBytes  int
	metadata     imagesResponse
	spoolTracker *helps.ChatGPTWebImageSpoolFile
	done         bool
	err          *interfaces.ErrorMessage
	ack          chan struct{}
}

type parallelWebImageResult struct {
	path         string
	size         int64
	headers      http.Header
	metadata     imagesResponse
	spoolTracker *helps.ChatGPTWebImageSpoolFile
}

func removeWebImageSpool(path string, tracker *helps.ChatGPTWebImageSpoolFile) {
	var err error
	if path != "" {
		err = os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if err != nil {
			log.WithError(err).Warn("remove Web image aggregation spool")
		}
	}
	if tracker != nil {
		tracker.FinishCleanup(err)
	}
}

func sendParallelWebImageEvent(ctx context.Context, events chan<- parallelWebImageEvent, event parallelWebImageEvent) bool {
	event.ack = make(chan struct{})
	select {
	case events <- event:
	case <-ctx.Done():
		return false
	}
	// The receiver owns a spool once the event has been delivered, even if the
	// batch is canceled while its acknowledgement is pending.
	select {
	case <-event.ack:
	case <-ctx.Done():
	}
	return true
}

func (h *OpenAIImagesAPIHandler) handleParallelWebImages(c *gin.Context, rawJSON []byte, imageModel, codexModel string, req openAIImageRequest, op imageOperation, count int, responseFormat string, ignoreUnsupported bool, snapshot core.ChatGPTWebImageConfigSnapshot, concurrency int) {
	batchCtx, cancel := context.WithCancel(auth.WithChatGPTWebImageAggregation(c.Request.Context()))
	events := make(chan parallelWebImageEvent)
	var wg sync.WaitGroup
	var next atomic.Int32
	results := make([]parallelWebImageResult, count)
	// Copy Gin contexts before workers start. Each child owns its request body
	// controller, error source, and finalization leases; only the caller writes HTTP.
	children := make([]*gin.Context, count)
	for i := range children {
		children[i] = c.Copy()
		children[i].Request = c.Request.Clone(batchCtx)
	}
	stopKeepAlive := func() {}
	var flusher http.Flusher
	if req.Stream {
		var ok bool
		flusher, ok = c.Writer.(http.Flusher)
		if !ok {
			cancel()
			h.writeImagesError(c, http.StatusInternalServerError, errors.New("streaming not supported"))
			return
		}
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
	} else {
		c.Header("Content-Type", "application/json")
		stopKeepAlive = h.StartNonStreamingKeepAlive(c, batchCtx)
	}
	defer func() {
		cancel()
		wg.Wait()
		stopKeepAlive()
		for _, result := range results {
			removeWebImageSpool(result.path, result.spoolTracker)
		}
	}()
	for worker := 0; worker < concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batchCtx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				h.executeParallelWebImage(batchCtx, children[index], events, index, rawJSON, imageModel, codexModel, req, count, ignoreUnsupported, snapshot)
			}
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(events); close(finished) }()
	limit, used, completed := imageResponseByteLimit(snapshot), 0, 0
	mappers := make(map[int]*imageStreamMapper)
	headersWritten := false
	flushInterval := time.Duration(0)
	if interval := imageStreamFlushInterval(h.Cfg); interval != nil {
		flushInterval = *interval
	}
	flushMinBytes := imageStreamFlushMinBytes(h.Cfg)
	var flushTimer *time.Timer
	var flushC <-chan time.Time
	pendingBytes := 0
	stopFlushTimer := func() {
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
			flushC = nil
		}
	}
	defer stopFlushTimer()
	flushPending := func() {
		if pendingBytes > 0 {
			flusher.Flush()
			pendingBytes = 0
		}
		stopFlushTimer()
	}
	noteWrite := func(bytes int, force bool) {
		pendingBytes += max(bytes, 0)
		if pendingBytes == 0 {
			return
		}
		if force || flushInterval <= 0 && flushMinBytes <= 0 || flushMinBytes > 0 && pendingBytes >= flushMinBytes {
			flushPending()
			return
		}
		if flushInterval > 0 && flushTimer == nil {
			flushTimer = time.NewTimer(flushInterval)
			flushC = flushTimer.C
		}
	}
	var keepAliveC <-chan time.Time
	if req.Stream {
		if interval := handlers.StreamingKeepAliveInterval(h.Cfg); interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			keepAliveC = ticker.C
		}
	}
	writeError := func(errMsg *interfaces.ErrorMessage) {
		cancel()
		stopKeepAlive()
		if req.Stream {
			flushPending()
			body, _ := h.BuildPublicErrorResponseBody(c, errMsg)
			observeImageResponseWrite(c, func() { _, _ = fmt.Fprintf(c.Writer, "\nevent: error\ndata: %s\n\n", body); flusher.Flush() })
		} else {
			h.WriteErrorResponse(c, errMsg)
		}
	}
	processingError := func(ctx context.Context, err error) *interfaces.ErrorMessage {
		return h.RewriteExecutionErrorResponseForContext(ctx, imageConversionErrorMessage(markChatGPTWebImageProcessingError(err, true)))
	}
receiveEvents:
	for {
		var event parallelWebImageEvent
		select {
		case nextEvent, ok := <-events:
			if !ok {
				break receiveEvents
			}
			event = nextEvent
		case <-flushC:
			flushPending()
			continue
		case <-keepAliveC:
			before := max(c.Writer.Size(), 0)
			observeImageResponseWrite(c, func() { _, _ = c.Writer.Write([]byte(": keep-alive\n\n")) })
			noteWrite(c.Writer.Size()-before, true)
			continue
		case <-batchCtx.Done():
			return
		}
		if event.path != "" {
			results[event.index] = parallelWebImageResult{event.path, event.size, event.headers, event.metadata, event.spoolTracker}
		}
		if event.err != nil {
			cancel()
			close(event.ack)
			writeError(event.err)
			return
		}
		if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
			cancel()
			close(event.ack)
			writeError(timeout)
			return
		}
		if batchCtx.Err() != nil {
			close(event.ack)
			return
		}
		if !req.Stream {
			used += event.binaryBytes
			if used > limit {
				cancel()
				close(event.ack)
				writeError(processingError(event.ctx, imageResponseBudgetError(limit)))
				return
			}
			completed++
		} else {
			mapper := mappers[event.index]
			if mapper == nil {
				index := event.index
				mapper = &imageStreamMapper{requestContext: event.ctx, operation: op, responseFormat: responseFormat, maxResults: 1, maxBinaryBytes: limit, omitInputUsage: index > 0, imageIndex: &index, projectWebImageErrors: true, parser: imageSSEParser{maxPendingBytes: imageSSEPendingByteLimit(snapshot)}}
				mapper.reserveResultBytes = func(bytes int) bool {
					if bytes > limit-used {
						return false
					}
					used += bytes
					return true
				}
				mappers[index] = mapper
				if !headersWritten {
					handlers.WriteUpstreamHeaders(c.Writer.Header(), event.headers)
					headersWritten = true
				}
			}
			beforeWrite := max(c.Writer.Size(), 0)
			observeImageResponseWrite(c, func() {
				if event.done {
					mapper.flush(c.Writer)
				} else {
					mapper.writeChunk(c.Writer, event.chunk)
				}
			})
			noteWrite(c.Writer.Size()-beforeWrite, event.done || mapper.consumeForceFlush())
			if err := mapper.fatalError(); err != nil {
				cancel()
				close(event.ack)
				writeError(processingError(event.ctx, err))
				return
			}
			if event.done {
				completed++
				delete(mappers, event.index)
			}
		}
		close(event.ack)
	}
	<-finished
	stopKeepAlive()
	if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
		writeError(timeout)
		return
	}
	if batchCtx.Err() != nil {
		return
	}
	if completed != count {
		writeError(processingError(batchCtx, errors.New("image aggregation ended before all images completed")))
		return
	}
	if req.Stream {
		flushPending()
		return
	}
	// Assemble directly from bounded spools. Re-decoding all base64 strings
	// would duplicate the entire batch several times before writing HTTP.
	finalCtx, finish := h.GetContextWithCancel(h, c, batchCtx)
	defer finish()
	leases := helps.NewChatGPTWebImageChildMemoryLeaseSet(estimateImageRequestResidentBytes(c.Request))
	defer leases.Release()
	releaseTurn, err := leases.BeginFinalization(finalCtx)
	if err != nil {
		writeError(processingError(finalCtx, err))
		return
	}
	defer releaseTurn()
	if err = leases.Acquire(finalCtx, 64<<10); err != nil {
		writeError(processingError(finalCtx, err))
		return
	}
	// Streaming disk bytes to a slow client must not hold the global finalizer turn.
	releaseTurn()
	combined := results[0].metadata
	combined.Usage = nil
	files := make([]*os.File, 0, len(results))
	defer func() {
		for _, file := range files {
			if errClose := file.Close(); errClose != nil {
				log.Warn("close Web image aggregation spool failed")
			}
		}
	}()
	for _, result := range results {
		combined.Usage = mergeImageUsageForNAggregation(combined.Usage, result.metadata.Usage)
		file, errOpen := os.Open(result.path)
		if errOpen != nil {
			writeError(processingError(finalCtx, errors.New("read image aggregation spool failed")))
			return
		}
		files = append(files, file)
		stat, errStat := file.Stat()
		if errStat != nil || stat.Size() != result.size {
			writeError(processingError(finalCtx, errors.New("invalid image aggregation spool")))
			return
		}
	}
	started := time.Now()
	header, err := json.Marshal(combined)
	if err == nil {
		header, err = sjson.DeleteBytes(header, "data")
	}
	observeImageRequestPhase(c, core.ImagePhaseResponseEncode, started)
	if err != nil {
		writeError(processingError(finalCtx, err))
		return
	}
	if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
		writeError(timeout)
		return
	}
	readers := []io.Reader{bytes.NewReader(header[:len(header)-1]), strings.NewReader(",\"data\":[")}
	for i, file := range files {
		if i > 0 {
			readers = append(readers, strings.NewReader(","))
		}
		readers = append(readers, file)
	}
	readers = append(readers, strings.NewReader("]}"))
	handlers.WriteUpstreamHeaders(c.Writer.Header(), results[0].headers)
	observeImageResponseWrite(c, func() {
		_, err = io.CopyBuffer(webImageContextWriter{finalCtx, c.Writer}, io.MultiReader(readers...), make([]byte, 32<<10))
	})
	if err != nil {
		cancel()
		log.Warn("Web image aggregation response write interrupted")
	}

}

func (h *OpenAIImagesAPIHandler) executeParallelWebImage(batchCtx context.Context, child *gin.Context, events chan<- parallelWebImageEvent, index int, rawJSON []byte, imageModel, codexModel string, req openAIImageRequest, count int, ignoreUnsupported bool, snapshot core.ChatGPTWebImageConfigSnapshot) {
	leases := helps.NewChatGPTWebImageChildMemoryLeaseSet(estimateImageRequestResidentBytes(child.Request))
	child.Set(helps.ChatGPTWebImageMemoryLeaseSetMetadataKey, leases)
	defer leases.Release()
	ctx, finish := h.GetContextWithCancel(h, child, batchCtx)
	var finishOnce sync.Once
	finishChild := func() { finishOnce.Do(func() { finish() }) }
	defer finishChild()
	ctx = handlers.WithChatGPTWebImageRequestCount(ctx, count)
	ctx = handlers.WithImageGenerationMaxResults(ctx, 1)
	ctx = handlers.WithChatGPTWebIgnoreUnsupportedImageParams(ctx, ignoreUnsupported)
	ctx = handlers.WithChatGPTWebImageConfigSnapshot(ctx, snapshot.Clone())
	fail := func(err *interfaces.ErrorMessage) {
		sendParallelWebImageEvent(batchCtx, events, parallelWebImageEvent{index: index, ctx: ctx, err: err})
	}
	localError := func(err error) {
		fail(h.RewriteExecutionErrorResponseForContext(ctx, imageConversionErrorMessage(markChatGPTWebImageProcessingError(err, true))))
	}
	if req.Stream {
		ctx = handlers.WithImageGenerationStreamPassthrough(ctx, true)
		data, headers, errs := h.ExecuteStreamWithProvidersAndExecutionModel(ctx, []string{ChatGPTWeb}, h.HandlerType(), imageModel, codexModel, rawJSON, "")
		defer func() {
			finishChild()
			// Drain the request-owned forwarding goroutines before releasing their buffers.
			for data != nil || errs != nil {
				select {
				case _, ok := <-data:
					if !ok {
						data = nil
					}
				case _, ok := <-errs:
					if !ok {
						errs = nil
					}
				}
			}
		}()
		for data != nil || errs != nil {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-errs:
				if !ok {
					errs = nil
					continue
				}
				if err != nil {
					fail(err)
					return
				}
			case chunk, ok := <-data:
				if !ok {
					data = nil
					continue
				}
				if !sendParallelWebImageEvent(batchCtx, events, parallelWebImageEvent{index: index, ctx: ctx, chunk: chunk, headers: headers}) {
					return
				}
			}
		}
		sendParallelWebImageEvent(batchCtx, events, parallelWebImageEvent{index: index, ctx: ctx, headers: headers, done: true})
		return
	}
	payload, headers, errMsg := h.ExecuteWithProvidersAndExecutionModel(ctx, []string{ChatGPTWeb}, h.HandlerType(), imageModel, codexModel, rawJSON, "")
	if errMsg != nil {
		fail(errMsg)
		return
	}
	parsed, err := parseResponsesToImagesResponse(payload, time.Now().Unix())
	if err != nil {
		localError(err)
		return
	}
	if len(parsed.Data) > 1 {
		parsed.Data = parsed.Data[:1]
	}
	applyRequestedImageMetadata(&parsed, req)
	bytes, err := imageResultsBinaryBytes(parsed.Data)
	if err != nil {
		localError(err)
		return
	}
	file, err := os.CreateTemp("", "cpa-web-image-aggregate-*")
	if err != nil {
		localError(errors.New("create image aggregation spool failed"))
		return
	}
	path := file.Name()
	spoolTracker := helps.BeginChatGPTWebImageSpool()
	transferred := false
	defer func() {
		if !transferred {
			removeWebImageSpool(path, spoolTracker)
		}
	}()
	applyImageResponseFormat(&parsed, req.ResponseFormat)
	errEncode := json.NewEncoder(file).Encode(parsed.Data[0])
	parsed.Data = nil
	stat, errStat := file.Stat()
	errClose := file.Close()
	if errEncode != nil || errStat != nil || errClose != nil {
		localError(errors.New("write image aggregation spool failed"))
		return
	}
	spoolTracker.SetBytes(stat.Size())
	transferred = sendParallelWebImageEvent(batchCtx, events, parallelWebImageEvent{index: index, ctx: ctx, path: path, size: stat.Size(), binaryBytes: bytes, headers: headers, done: true, metadata: parsed, spoolTracker: spoolTracker})
}

// webImageContextWriter checks the existing logical budget between disk chunks.
type webImageContextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w webImageContextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}
