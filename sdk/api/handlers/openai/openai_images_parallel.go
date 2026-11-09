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
	"github.com/google/uuid"
	. "github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	helps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/sjson"
)

// Mixed-provider and native routes keep their existing aggregation semantics.
func (h *OpenAIImagesAPIHandler) webImageAggregationConcurrency(c *gin.Context, providers []string, count int, snapshot core.ChatGPTWebImageConfigSnapshot) int {
	if count < 2 || h.AuthManager == nil || nativeImageRequest(c) != nil || !imageProvidersContain(providers, ChatGPTWeb) {
		return 0
	}
	if !chatGPTWebAllowedWithoutCodex(c) {
		for _, provider := range providers {
			if provider != ChatGPTWeb {
				if h.AuthManager.HasProviderCredentials(provider) {
					return 0
				}
			}
		}
	}
	return min(count, max(1, snapshot.NAggregationConcurrency))
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
	defer cancel()
	slots := make([]webImageAggregationSlot, count)
	batchID := uuid.NewString()
	accounting := newWebImageBatchAccounting(count, batchID, imageModel)
	accountingCtx, finishAccounting := h.GetContextWithCancel(h, c, batchCtx)
	defer finishAccounting()
	batchCtx = usage.WithImageAggregationObserver(accountingCtx, accounting.observe)
	var retryStarts atomic.Int32
	defer func() { accounting.publish(accountingCtx, slots, int(retryStarts.Load())) }()
	// The immutable canonical input remains owned by the parent input lease until
	// every round ends. Child body-release controllers only release their own references.
	defer func() {
		for _, slot := range slots {
			removeWebImageSpool(slot.result.path, slot.result.spoolTracker)
		}
	}()
	stopKeepAlive := func() {}
	var flusher http.Flusher
	if req.Stream {
		var ok bool
		flusher, ok = c.Writer.(http.Flusher)
		if !ok {
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
	defer stopKeepAlive()
	writeError := func(msg *interfaces.ErrorMessage) {
		stopKeepAlive()
		if req.Stream {
			body, _ := h.BuildPublicErrorResponseBody(c, msg)
			observeImageResponseWrite(c, func() { _, _ = fmt.Fprintf(c.Writer, "\nevent: error\ndata: %s\n\n", body); flusher.Flush() })
		} else {
			h.WriteErrorResponse(c, msg)
		}
	}
	processingError := func(ctx context.Context, err error) *interfaces.ErrorMessage {
		return h.RewriteExecutionErrorResponseForContext(ctx, imageConversionErrorMessage(markChatGPTWebImageProcessingError(err, true)))
	}
	var heartbeat <-chan time.Time
	if req.Stream {
		if interval := handlers.StreamingKeepAliveInterval(h.Cfg); interval > 0 {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			heartbeat = ticker.C
		}
	}
	flushInterval := time.Duration(0)
	if interval := imageStreamFlushInterval(h.Cfg); interval != nil {
		flushInterval = *interval
	}
	flushMinBytes := imageStreamFlushMinBytes(h.Cfg)
	var flushTimer *time.Timer
	var flushC <-chan time.Time
	pendingBytes := 0
	flushPending := func() {
		if pendingBytes > 0 {
			flusher.Flush()
			pendingBytes = 0
		}
		if flushTimer != nil {
			flushTimer.Stop()
			flushTimer = nil
			flushC = nil
		}
	}
	defer func() {
		if flushTimer != nil {
			flushTimer.Stop()
		}
	}()
	noteWrite := func(n int, force bool) {
		pendingBytes += max(n, 0)
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
	used := 0
	var retainedBytes atomic.Int64
	inputUsageSent := false
	pending := make([]int, count)
	for i := range pending {
		pending[i] = i
	}
	for attempt := 0; len(pending) > 0; attempt++ {
		events := make(chan parallelWebImageEvent)
		var wg sync.WaitGroup
		var next atomic.Int32
		children := make(map[int]*gin.Context, len(pending))
		cancels := make(map[int]context.CancelFunc, len(pending))
		for _, index := range pending {
			ctx, stop := context.WithCancel(core.WithImageAggregationAttempt(batchCtx, core.ImageAggregationAttempt{BatchID: batchID, ImageIndex: index, AttemptIndex: attempt, Requested: count}))
			child := c.Copy()
			child.Request = c.Request.Clone(ctx)
			children[index], cancels[index] = child, stop
			slots[index].err = nil
			slots[index].attempt = attempt
		}
		roundPending := pending
		for worker := 0; worker < min(concurrency, len(roundPending)); worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for batchCtx.Err() == nil {
					position := int(next.Add(1) - 1)
					if position >= len(roundPending) {
						return
					}
					index := roundPending[position]
					if attempt > 0 {
						retryStarts.Add(1)
					}
					accounting.start(index)
					attemptSnapshot := snapshot.Clone()
					remaining := imageResponseByteLimit(snapshot) - int(retainedBytes.Load())
					if remaining <= 0 {
						sendParallelWebImageEvent(batchCtx, events, parallelWebImageEvent{index: index, err: processingError(children[index].Request.Context(), imageResponseBudgetError(imageResponseByteLimit(snapshot)))})
						continue
					}
					attemptSnapshot.MaxImageResponseBytes = remaining
					h.executeParallelWebImage(children[index].Request.Context(), children[index], events, index, rawJSON, imageModel, codexModel, req, count, ignoreUnsupported, attemptSnapshot)
				}
			}()
		}
		go func() { wg.Wait(); close(events) }()
		mappers := make(map[int]*imageStreamMapper)
		var stopErr *interfaces.ErrorMessage
		writer := &webImageDeliveryWriter{ctx: batchCtx, writer: c.Writer}
		for {
			var event parallelWebImageEvent
			var ok bool
			select {
			case event, ok = <-events:
				if !ok {
					goto roundFinished
				}
			case <-flushC:
				flushPending()
				continue
			case <-heartbeat:
				if batchCtx.Err() == nil {
					_, _ = writer.Write([]byte(": keep-alive\n\n"))
					flusher.Flush()
					if writer.err != nil {
						cancel()
					}
				}
				continue
			}
			slot := &slots[event.index]
			if event.path != "" {
				slot.result = parallelWebImageResult{event.path, event.size, event.headers, event.metadata, event.spoolTracker}
			}
			if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
				stopErr = timeout
				cancel()
			}
			if batchCtx.Err() != nil {
				close(event.ack)
				continue
			}
			if slot.err != nil || slot.generated {
				close(event.ack)
				continue
			}
			if event.err != nil {
				slot.err = event.err
				if webImageBatchStopped(event.err) {
					stopErr = event.err
					cancel()
				}
				cancels[event.index]()
			} else if !req.Stream {
				if event.binaryBytes > imageResponseByteLimit(snapshot)-used {
					slot.err = processingError(event.ctx, imageResponseBudgetError(imageResponseByteLimit(snapshot)))
					removeWebImageSpool(slot.result.path, slot.result.spoolTracker)
					slot.result = parallelWebImageResult{}
				} else {
					used += event.binaryBytes
					retainedBytes.Store(int64(used))
					slot.generated = true
				}
			} else {
				mapper := mappers[event.index]
				if mapper == nil {
					index := event.index
					mapper = &imageStreamMapper{requestContext: event.ctx, operation: op, responseFormat: responseFormat, maxResults: 1, maxBinaryBytes: imageResponseByteLimit(snapshot), imageIndex: &index, attemptIndex: &slot.attempt, projectWebImageErrors: true, parser: imageSSEParser{maxPendingBytes: imageSSEPendingByteLimit(snapshot)}}
					mapper.reserveResultBytes = func(n int) bool {
						if n > imageResponseByteLimit(snapshot)-used {
							return false
						}
						used += n
						retainedBytes.Store(int64(used))
						return true
					}
					mapper.onCompleted = func() { slot.generated = true; slot.delivered = true; inputUsageSent = true }
					mappers[index] = mapper
				}
				mapper.omitInputUsage = inputUsageSent
				beforeWrite := max(c.Writer.Size(), 0)
				observeImageResponseWrite(c, func() {
					if event.done {
						mapper.flush(writer)
					} else {
						mapper.writeChunk(writer, event.chunk)
					}
				})
				noteWrite(c.Writer.Size()-beforeWrite, mapper.consumeForceFlush() || event.done || slot.delivered)
				if writer.err != nil {
					cancel()
				} else if err := mapper.fatalError(); err != nil {
					slot.err = processingError(event.ctx, err)
					cancels[event.index]()
				}
			}
			close(event.ack)
		}
	roundFinished:
		for _, stop := range cancels {
			stop()
		}
		if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
			stopErr = timeout
		}
		if stopErr != nil {
			writeError(stopErr)
			return
		}
		if batchCtx.Err() != nil {
			return
		}
		pending = nil
		succeeded := 0
		for i := range slots {
			if slots[i].generated {
				succeeded++
			} else {
				pending = append(pending, i)
			}
		}
		if len(pending) == 0 || attempt >= snapshot.NAggregationMaxRetries || (attempt == 0 && !webImageCanRefill(count, succeeded)) {
			break
		}
	}
	stopKeepAlive()
	summary := webImageBatchSummary(slots, int(retryStarts.Load()))
	if req.Stream {
		flushPending()
		if summary.Generated < count {
			writeError(h.RewriteExecutionErrorResponseForContext(batchCtx, handlers.ExecutionErrorMessage(&webImageAggregationError{summary: summary})))
		}
		return
	}
	if summary.Generated == 0 || summary.Generated < count && !snapshot.NAggregationReturnPartial {
		writeError(h.RewriteExecutionErrorResponseForContext(batchCtx, handlers.ExecutionErrorMessage(&webImageAggregationError{summary: summary})))
		return
	}
	successful := make([]parallelWebImageResult, 0, summary.Generated)
	for _, slot := range slots {
		if slot.generated {
			successful = append(successful, slot.result)
		}
	}
	summary.Delivered = summary.Generated
	if h.writeWebImageBatch(c, batchCtx, successful, summary, writeError) {
		for i := range slots {
			slots[i].delivered = slots[i].generated
		}
	}
}

func (h *OpenAIImagesAPIHandler) writeWebImageBatch(c *gin.Context, batchCtx context.Context, results []parallelWebImageResult, summary *webImageAggregationSummary, writeError func(*interfaces.ErrorMessage)) bool {
	processingError := func(ctx context.Context, err error) *interfaces.ErrorMessage {
		return h.RewriteExecutionErrorResponseForContext(ctx, imageConversionErrorMessage(markChatGPTWebImageProcessingError(err, true)))
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
		return false
	}
	defer releaseTurn()
	if err = leases.Acquire(finalCtx, 64<<10); err != nil {
		writeError(processingError(finalCtx, err))
		return false
	}
	// Streaming disk bytes to a slow client must not hold the global finalizer turn.
	releaseTurn()
	combined := results[0].metadata
	combined.Usage = nil
	combined.Aggregation = summary
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
			return false
		}
		files = append(files, file)
		stat, errStat := file.Stat()
		if errStat != nil || stat.Size() != result.size {
			writeError(processingError(finalCtx, errors.New("invalid image aggregation spool")))
			return false
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
		return false
	}
	if timeout := h.ImageRequestTimeoutResponseForGin(c); timeout != nil {
		writeError(timeout)
		return false
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
		log.Warn("Web image aggregation response write interrupted")
		return false
	}
	return true
}

func (h *OpenAIImagesAPIHandler) executeParallelWebImage(batchCtx context.Context, child *gin.Context, events chan<- parallelWebImageEvent, index int, rawJSON []byte, imageModel, codexModel string, req openAIImageRequest, count int, ignoreUnsupported bool, snapshot core.ChatGPTWebImageConfigSnapshot) {
	leases := helps.NewChatGPTWebImageChildMemoryLeaseSet(max(estimateImageRequestResidentBytes(child.Request), int64(len(rawJSON))))
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
				fail(handlers.ExecutionErrorMessage(core.ImageRequestContextError(ctx, ctx.Err())))
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
	if len(parsed.Data) == 0 {
		localError(errors.New("upstream did not return image output"))
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
