package executor

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"image"
	"image/gif"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const chatGPTWebRemoteImagePhaseTimeout = 30 * time.Second

type chatGPTWebRemoteImageStatusError struct {
	statusErr
	code string
}

func (err *chatGPTWebRemoteImageStatusError) ExecutionResultErrorCode() string {
	if err == nil {
		return "remote_image_fetch_failed"
	}
	return err.code
}

func (*chatGPTWebRemoteImageStatusError) ChatGPTWebFailureStage() string { return "input_download" }

type chatGPTWebRemoteImageMetadata struct {
	file          chatGPTWebRemoteImageFile
	mimeType      string
	width         int
	height        int
	decodedPixels int64
	gifFrames     int
	dataURL       string
}

type chatGPTWebRemoteImageFile interface {
	WithReader(func(io.Reader) error) error
	Remove() error
	SizeBytes() int64
	ContentType() string
}

type chatGPTWebRemoteImageFetchFunc func(context.Context, string, string) (chatGPTWebRemoteImageFile, error)

func newChatGPTWebRemoteImageStatusError(status int, code, message string) error {
	payload, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message": message,
		"type":    "invalid_request_error",
		"param":   "image_url",
		"code":    code,
	}})
	return &chatGPTWebRemoteImageStatusError{
		statusErr: statusErr{code: status, msg: string(payload), skipAuthResult: true, retryOtherAuth: false},
		code:      code,
	}
}

func chatGPTWebRemoteImageFetchStatusError(err error) error {
	var remoteErr *helps.ChatGPTWebRemoteImageError
	if !errors.As(err, &remoteErr) || remoteErr == nil {
		return newChatGPTWebRemoteImageStatusError(http.StatusBadGateway, "remote_image_fetch_failed", "remote image download failed")
	}
	switch remoteErr.Kind {
	case helps.ChatGPTWebRemoteImageInvalid:
		return newChatGPTWebRemoteImageStatusError(http.StatusBadRequest, "remote_image_url_invalid", "remote image URL is invalid")
	case helps.ChatGPTWebRemoteImageBlocked:
		return newChatGPTWebRemoteImageStatusError(http.StatusBadRequest, "remote_image_url_blocked", "remote image URL is blocked")
	case helps.ChatGPTWebRemoteImageTooLarge:
		return newChatGPTWebRemoteImageStatusError(http.StatusRequestEntityTooLarge, "remote_image_too_large", "remote image exceeds 50 MiB")
	default:
		return newChatGPTWebRemoteImageStatusError(http.StatusBadGateway, "remote_image_fetch_failed", "remote image download failed")
	}
}

func chatGPTWebRemoteImageContentError() error {
	return newChatGPTWebRemoteImageStatusError(http.StatusBadRequest, "remote_image_invalid_content", "remote image content is invalid")
}

func chatGPTWebRemoteImageMemoryError() error {
	return newChatGPTWebRemoteImageStatusError(http.StatusServiceUnavailable, "image_memory_capacity", "image memory capacity is temporarily exhausted")
}

func chatGPTWebPreparedRequestHasRemoteImages(prepared *chatGPTWebPreparedRequest) bool {
	if prepared == nil || !prepared.imageConfigSnapshot.RemoteImageURLEnabled {
		return false
	}
	if prepared.request.Image != nil {
		for _, reference := range prepared.request.Image.Images {
			if helps.IsChatGPTWebRemoteImageURL(reference) {
				return true
			}
		}
		if helps.IsChatGPTWebRemoteImageURL(prepared.request.Image.MaskURL) {
			return true
		}
	}
	for _, message := range prepared.request.Messages {
		for _, part := range message.Parts {
			if helps.IsChatGPTWebRemoteImageURL(part.ImageURL) {
				return true
			}
		}
	}
	return false
}

func chatGPTWebImageRequestHasRemoteReference(request *helps.ChatGPTWebImageRequest) bool {
	if request == nil {
		return false
	}
	for _, reference := range request.Images {
		if helps.IsChatGPTWebRemoteImageURL(reference) {
			return true
		}
	}
	return helps.IsChatGPTWebRemoteImageURL(request.MaskURL)
}

func validateChatGPTWebImageReferences(references []string, allowRemote bool) error {
	if len(references) > helps.ChatGPTWebMaxImageInputs {
		return statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            "chatgpt web image inputs exceed 16 items",
			skipAuthResult: true,
		}
	}
	localReferences := make([]string, 0, len(references))
	for _, rawReference := range references {
		reference := strings.TrimSpace(rawReference)
		if helps.IsChatGPTWebRemoteImageURL(reference) {
			if !allowRemote {
				return statusErr{
					code:           http.StatusBadRequest,
					msg:            "chatgpt web only supports base64 image inputs",
					skipAuthResult: true,
					retryOtherAuth: true,
				}
			}
			if errURL := helps.ValidateChatGPTWebRemoteImageURL(reference); errURL != nil {
				return chatGPTWebRemoteImageFetchStatusError(errURL)
			}
			continue
		}
		if strings.Contains(reference, "://") && !strings.HasPrefix(strings.ToLower(reference), "data:") {
			if allowRemote {
				return newChatGPTWebRemoteImageStatusError(http.StatusBadRequest, "remote_image_url_invalid", "remote image URL is invalid")
			}
			return statusErr{
				code:           http.StatusBadRequest,
				msg:            "chatgpt web only supports base64 image inputs",
				skipAuthResult: true,
				retryOtherAuth: true,
			}
		}
		localReferences = append(localReferences, reference)
	}
	if err := helps.ValidateChatGPTWebImageReferences(localReferences, chatGPTWebMaxImageBytes, chatGPTWebMaxImageRequestBytes); err != nil {
		return statusErr{
			code:           http.StatusRequestEntityTooLarge,
			msg:            err.Error(),
			skipAuthResult: true,
		}
	}
	return nil
}

func (e *ChatGPTWebExecutor) materializeChatGPTWebRemoteImages(ctx context.Context, auth *cliproxyauth.Auth, prepared *chatGPTWebPreparedRequest) error {
	fetch := e.remoteImageFetcher
	if fetch == nil {
		fetch = func(fetchCtx context.Context, reference, proxyURL string) (chatGPTWebRemoteImageFile, error) {
			return helps.FetchChatGPTWebRemoteImageWithProxy(fetchCtx, reference, proxyURL)
		}
	}
	return e.materializeChatGPTWebRemoteImagesWithFetcher(ctx, prepared, chatGPTWebRemoteImageProxyURL(auth, prepared), fetch)
}

func (e *ChatGPTWebExecutor) materializeChatGPTWebRemoteImagesWithFetcher(
	ctx context.Context,
	prepared *chatGPTWebPreparedRequest,
	proxyURL string,
	fetch chatGPTWebRemoteImageFetchFunc,
) error {
	if !chatGPTWebPreparedRequestHasRemoteImages(prepared) {
		return nil
	}
	if fetch == nil || prepared.imageMemoryLeases == nil {
		return chatGPTWebRemoteImageMemoryError()
	}
	phaseCtx, cancel := context.WithTimeout(ctx, chatGPTWebRemoteImagePhaseTimeout)
	defer cancel()

	references := chatGPTWebPreparedImageReferences(prepared)
	if len(references) > helps.ChatGPTWebMaxImageInputs {
		return newChatGPTWebRemoteImageStatusError(
			http.StatusRequestEntityTooLarge,
			"remote_image_too_large",
			"image inputs exceed 16 items",
		)
	}
	metadata := make(map[string]*chatGPTWebRemoteImageMetadata)
	defer func() {
		for _, item := range metadata {
			_ = item.file.Remove()
		}
	}()
	for _, reference := range references {
		reference = strings.TrimSpace(reference)
		if !helps.IsChatGPTWebRemoteImageURL(reference) {
			continue
		}
		if _, exists := metadata[reference]; exists {
			continue
		}
		file, errFetch := fetch(phaseCtx, reference, proxyURL)
		if errFetch != nil {
			return chatGPTWebRemoteImageFetchStatusError(errFetch)
		}
		normalizeMIME := prepared.imageConfigSnapshot.NormalizeMismatchedImageMIME &&
			prepared.imageConfigSnapshot.NormalizeRemoteImageMIME
		item, errInspect := inspectChatGPTWebRemoteImage(file, normalizeMIME)
		if errInspect != nil {
			_ = file.Remove()
			var mismatch *helps.ChatGPTWebImageMIMEMismatchError
			if errors.As(errInspect, &mismatch) {
				return newChatGPTWebRemoteImageStatusError(
					http.StatusBadRequest,
					"remote_image_mime_mismatch",
					mismatch.Error(),
				)
			}
			return chatGPTWebRemoteImageContentError()
		}
		metadata[reference] = item
	}

	var totalInputBytes int64
	for _, reference := range references {
		reference = strings.TrimSpace(reference)
		if item := metadata[reference]; item != nil {
			if item.file.SizeBytes() > int64(helps.ChatGPTWebMaxImageBytes) {
				return newChatGPTWebRemoteImageStatusError(http.StatusRequestEntityTooLarge, "remote_image_too_large", "remote image exceeds 50 MiB")
			}
			if !safeChatGPTWebRemoteImageAdd(&totalInputBytes, item.file.SizeBytes(), int64(helps.ChatGPTWebMaxImageRequestBytes)) {
				return newChatGPTWebRemoteImageStatusError(http.StatusRequestEntityTooLarge, "remote_image_too_large", "image inputs exceed 100 MiB")
			}
			continue
		}
		size, errSize := helps.ChatGPTWebEncodedImageSize(reference, helps.ChatGPTWebMaxImageBytes)
		if errSize != nil || !safeChatGPTWebRemoteImageAdd(&totalInputBytes, int64(size), int64(helps.ChatGPTWebMaxImageRequestBytes)) {
			return newChatGPTWebRemoteImageStatusError(http.StatusRequestEntityTooLarge, "remote_image_too_large", "image inputs exceed 100 MiB")
		}
	}

	workingSet, okWorkingSet := chatGPTWebRemoteImageWorkingSet(prepared, metadata)
	capacity := helps.ChatGPTWebImageMemorySnapshot().CapacityBytes
	if !okWorkingSet || capacity < 1 || workingSet > capacity {
		return chatGPTWebRemoteImageMemoryError()
	}
	prepared.imageMemoryLeases.ReleaseInput()
	if !prepared.imageMemoryLeases.TryAcquireInputExact(workingSet) {
		return chatGPTWebRemoteImageMemoryError()
	}

	for _, item := range metadata {
		if errDecode := validateChatGPTWebRemoteImageDecode(item); errDecode != nil {
			return chatGPTWebRemoteImageContentError()
		}
		dataURL, errEncode := encodeChatGPTWebRemoteImageDataURL(item)
		if errEncode != nil {
			return newChatGPTWebRemoteImageStatusError(http.StatusBadGateway, "remote_image_fetch_failed", "remote image conversion failed")
		}
		item.dataURL = dataURL
	}
	replaceChatGPTWebRemoteImageReferences(prepared, metadata)
	if prepared.request.Image != nil && strings.TrimSpace(prepared.request.Image.MaskURL) != "" {
		if errMask := prepareChatGPTWebImageMask(prepared.request.Image); errMask != nil {
			return chatGPTWebRemoteImageContentError()
		}
	}
	return nil
}

func chatGPTWebRemoteImageProxyURL(auth *cliproxyauth.Auth, prepared *chatGPTWebPreparedRequest) string {
	if auth == nil || prepared == nil ||
		prepared.imageConfigSnapshot.RemoteImageURLDownloadMode != config.ChatGPTWebRemoteImageDownloadCredentialProxy {
		return ""
	}
	proxyURL := strings.TrimSpace(auth.EffectiveProxyURL())
	if strings.EqualFold(proxyURL, "direct") {
		return ""
	}
	return proxyURL
}

func chatGPTWebPreparedImageReferences(prepared *chatGPTWebPreparedRequest) []string {
	if prepared == nil {
		return nil
	}
	references := make([]string, 0, helps.ChatGPTWebMaxImageInputs)
	if prepared.request.Image != nil {
		references = append(references, prepared.request.Image.Images...)
		if mask := strings.TrimSpace(prepared.request.Image.MaskURL); mask != "" {
			references = append(references, mask)
		}
	}
	for _, message := range prepared.request.Messages {
		for _, part := range message.Parts {
			if reference := strings.TrimSpace(part.ImageURL); reference != "" {
				references = append(references, reference)
			}
		}
	}
	return references
}

func inspectChatGPTWebRemoteImage(file chatGPTWebRemoteImageFile, normalizeMIME bool) (*chatGPTWebRemoteImageMetadata, error) {
	if file == nil || file.SizeBytes() < 1 {
		return nil, errors.New("remote image file is empty")
	}
	var imageConfig image.Config
	var mimeType string
	errInspect := file.WithReader(func(reader io.Reader) error {
		var errDecode error
		imageConfig, mimeType, errDecode = helps.DetectChatGPTWebImageMIME(reader)
		return errDecode
	})
	if errInspect != nil {
		return nil, errInspect
	}
	if mimeType == "" || validateChatGPTWebImageConfig(imageConfig) != nil {
		return nil, errors.New("remote image format is unsupported")
	}
	if !chatGPTWebRemoteImageDeclaredMIMEMatches(file.ContentType(), mimeType) {
		if !normalizeMIME {
			declared := strings.TrimSpace(file.ContentType())
			if canonical := helps.CanonicalChatGPTWebImageMIME(declared); canonical != "" {
				declared = canonical
			}
			return nil, &helps.ChatGPTWebImageMIMEMismatchError{Declared: declared, Detected: mimeType}
		}
	}
	metadata := &chatGPTWebRemoteImageMetadata{
		file:          file,
		mimeType:      mimeType,
		width:         imageConfig.Width,
		height:        imageConfig.Height,
		decodedPixels: int64(imageConfig.Width) * int64(imageConfig.Height),
	}
	if mimeType == "image/gif" {
		frames, pixels, errGIF := inspectChatGPTWebRemoteGIF(file)
		if errGIF != nil {
			return nil, errGIF
		}
		metadata.gifFrames = frames
		metadata.decodedPixels = pixels
	}
	return metadata, nil
}

func inspectChatGPTWebRemoteGIF(file chatGPTWebRemoteImageFile) (int, int64, error) {
	if file == nil {
		return 0, 0, errors.New("remote GIF is unavailable")
	}
	var frames int
	var totalPixels int64
	errInspect := file.WithReader(func(reader io.Reader) error {
		buffered := bufio.NewReader(reader)
		header := make([]byte, 13)
		if _, errRead := io.ReadFull(buffered, header); errRead != nil ||
			(string(header[:6]) != "GIF87a" && string(header[:6]) != "GIF89a") {
			return errors.New("remote GIF header is invalid")
		}
		if errSkip := skipChatGPTWebRemoteGIFColorTable(buffered, header[10]); errSkip != nil {
			return errSkip
		}
		for {
			marker, errMarker := buffered.ReadByte()
			if errMarker != nil {
				return errors.New("remote GIF is truncated")
			}
			switch marker {
			case 0x3b:
				if frames == 0 {
					return errors.New("remote GIF has no frames")
				}
				if _, errTrailing := buffered.ReadByte(); !errors.Is(errTrailing, io.EOF) {
					return errors.New("remote GIF has trailing data")
				}
				return nil
			case 0x21:
				if _, errLabel := buffered.ReadByte(); errLabel != nil {
					return errors.New("remote GIF extension is truncated")
				}
				if errSkip := skipChatGPTWebRemoteGIFSubBlocks(buffered); errSkip != nil {
					return errSkip
				}
			case 0x2c:
				descriptor := make([]byte, 9)
				if _, errRead := io.ReadFull(buffered, descriptor); errRead != nil {
					return errors.New("remote GIF frame descriptor is truncated")
				}
				width := int64(binary.LittleEndian.Uint16(descriptor[4:6]))
				height := int64(binary.LittleEndian.Uint16(descriptor[6:8]))
				if width < 1 || height < 1 || width > math.MaxInt64/height {
					return errors.New("remote GIF frame dimensions are invalid")
				}
				pixels := width * height
				if pixels > chatGPTWebMaxImagePixels || totalPixels > int64(chatGPTWebMaxImagePixels)-pixels {
					return errors.New("remote GIF decoded pixels exceed limit")
				}
				totalPixels += pixels
				frames++
				if errSkip := skipChatGPTWebRemoteGIFColorTable(buffered, descriptor[8]); errSkip != nil {
					return errSkip
				}
				if _, errCodeSize := buffered.ReadByte(); errCodeSize != nil {
					return errors.New("remote GIF image data is truncated")
				}
				if errSkip := skipChatGPTWebRemoteGIFSubBlocks(buffered); errSkip != nil {
					return errSkip
				}
			default:
				return errors.New("remote GIF block is invalid")
			}
		}
	})
	return frames, totalPixels, errInspect
}

func skipChatGPTWebRemoteGIFColorTable(reader io.Reader, packed byte) error {
	if packed&0x80 == 0 {
		return nil
	}
	entries := 1 << ((packed & 0x07) + 1)
	if _, errSkip := io.CopyN(io.Discard, reader, int64(entries*3)); errSkip != nil {
		return errors.New("remote GIF color table is truncated")
	}
	return nil
}

func skipChatGPTWebRemoteGIFSubBlocks(reader *bufio.Reader) error {
	for {
		size, errSize := reader.ReadByte()
		if errSize != nil {
			return errors.New("remote GIF data is truncated")
		}
		if size == 0 {
			return nil
		}
		if _, errSkip := io.CopyN(io.Discard, reader, int64(size)); errSkip != nil {
			return errors.New("remote GIF data is truncated")
		}
	}
}

func chatGPTWebImageFormatMIME(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	case "gif":
		return "image/gif"
	case "webp":
		return "image/webp"
	default:
		return ""
	}
}

func chatGPTWebRemoteImageDeclaredMIMEMatches(declared, detected string) bool {
	declared = strings.TrimSpace(declared)
	if declared == "" {
		return true
	}
	mediaType, _, errParse := mime.ParseMediaType(declared)
	if errParse != nil {
		return false
	}
	mediaType = strings.ToLower(strings.TrimSpace(mediaType))
	if mediaType == "application/octet-stream" {
		return true
	}
	if mediaType == "image/jpg" {
		mediaType = "image/jpeg"
	}
	return mediaType == detected
}

func validateChatGPTWebRemoteImageDecode(item *chatGPTWebRemoteImageMetadata) error {
	if item == nil || item.file == nil {
		return errors.New("remote image metadata is unavailable")
	}
	return item.file.WithReader(func(reader io.Reader) error {
		if item.mimeType == "image/gif" {
			decoded, errDecode := gif.DecodeAll(reader)
			if errDecode != nil || decoded == nil || len(decoded.Image) != item.gifFrames ||
				decoded.Config.Width != item.width || decoded.Config.Height != item.height {
				return errors.New("remote GIF cannot be decoded")
			}
			var pixels int64
			for _, frame := range decoded.Image {
				bounds := frame.Bounds()
				framePixels := int64(bounds.Dx()) * int64(bounds.Dy())
				if framePixels < 1 || pixels > item.decodedPixels-framePixels {
					return errors.New("remote GIF frame dimensions changed during decode")
				}
				pixels += framePixels
			}
			if pixels != item.decodedPixels {
				return errors.New("remote GIF decoded pixels changed")
			}
			return nil
		}
		decoded, format, errDecode := image.Decode(reader)
		if errDecode != nil || chatGPTWebImageFormatMIME(format) != item.mimeType {
			return errors.New("remote image cannot be decoded")
		}
		bounds := decoded.Bounds()
		if bounds.Dx() != item.width || bounds.Dy() != item.height {
			return errors.New("remote image dimensions changed during decode")
		}
		return nil
	})
}

func encodeChatGPTWebRemoteImageDataURL(item *chatGPTWebRemoteImageMetadata) (string, error) {
	if item == nil || item.file == nil || item.file.SizeBytes() < 1 || item.mimeType == "" {
		return "", errors.New("remote image metadata is unavailable")
	}
	prefix := "data:" + item.mimeType + ";base64,"
	encodedSize := base64.StdEncoding.EncodedLen(int(item.file.SizeBytes()))
	if encodedSize < 0 || encodedSize > int(^uint(0)>>1)-len(prefix) {
		return "", errors.New("remote image encoding is too large")
	}
	var builder strings.Builder
	builder.Grow(len(prefix) + encodedSize)
	builder.WriteString(prefix)
	errEncode := item.file.WithReader(func(reader io.Reader) error {
		encoder := base64.NewEncoder(base64.StdEncoding, &builder)
		_, errCopy := io.Copy(encoder, reader)
		errClose := encoder.Close()
		if errCopy != nil {
			return errCopy
		}
		return errClose
	})
	if errEncode != nil {
		return "", errEncode
	}
	return builder.String(), nil
}

func chatGPTWebRemoteImageWorkingSet(
	prepared *chatGPTWebPreparedRequest,
	metadata map[string]*chatGPTWebRemoteImageMetadata,
) (int64, bool) {
	workingSet := int64(len(prepared.originalPayload) + len(prepared.canonicalBody))
	for _, item := range metadata {
		if item == nil || item.file == nil {
			return 0, false
		}
		encodedBytes := int64(base64.StdEncoding.EncodedLen(int(item.file.SizeBytes()))) + int64(len("data:"+item.mimeType+";base64,"))
		decodedBytes := item.decodedPixels * chatGPTWebDecodedImageBytesPerPixel
		for _, value := range []int64{item.file.SizeBytes(), encodedBytes, decodedBytes} {
			if value < 0 || workingSet > math.MaxInt64-value {
				return 0, false
			}
			workingSet += value
		}
	}
	for _, reference := range chatGPTWebPreparedImageReferences(prepared) {
		reference = strings.TrimSpace(reference)
		if metadata[reference] != nil {
			continue
		}
		rawBytes, imageConfig, okReference := chatGPTWebLocalImageReferenceConfig(reference)
		if !okReference {
			return 0, false
		}
		decodedBytes := int64(imageConfig.Width) * int64(imageConfig.Height) * chatGPTWebDecodedImageBytesPerPixel
		for _, value := range []int64{int64(len(reference)), rawBytes, decodedBytes} {
			if value < 0 || workingSet > math.MaxInt64-value {
				return 0, false
			}
			workingSet += value
		}
	}
	if prepared.request.Image != nil && strings.TrimSpace(prepared.request.Image.MaskURL) != "" {
		if workingSet > math.MaxInt64-chatGPTWebMaxImageEditDecodedBytes {
			return 0, false
		}
		workingSet += chatGPTWebMaxImageEditDecodedBytes
	}
	return workingSet, true
}

func chatGPTWebLocalImageReferenceConfig(reference string) (int64, image.Config, bool) {
	reference = strings.TrimSpace(reference)
	rawBytes, errSize := helps.ChatGPTWebEncodedImageSize(reference, helps.ChatGPTWebMaxImageBytes)
	if errSize != nil {
		return 0, image.Config{}, false
	}
	payload := reference
	if strings.HasPrefix(strings.ToLower(reference), "data:") {
		comma := strings.IndexByte(reference, ',')
		if comma < 0 || !strings.Contains(strings.ToLower(reference[:comma]), ";base64") {
			return 0, image.Config{}, false
		}
		payload = reference[comma+1:]
	}
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(payload))
	imageConfig, format, errConfig := image.DecodeConfig(decoder)
	if errConfig != nil || chatGPTWebImageFormatMIME(format) == "" || validateChatGPTWebImageConfig(imageConfig) != nil {
		return 0, image.Config{}, false
	}
	return int64(rawBytes), imageConfig, true
}

func safeChatGPTWebRemoteImageAdd(total *int64, value, limit int64) bool {
	if total == nil || value < 0 || limit < 0 || *total > limit-value {
		return false
	}
	*total += value
	return true
}

func replaceChatGPTWebRemoteImageReferences(
	prepared *chatGPTWebPreparedRequest,
	metadata map[string]*chatGPTWebRemoteImageMetadata,
) {
	if prepared == nil {
		return
	}
	if prepared.request.Image != nil {
		for index, reference := range prepared.request.Image.Images {
			if item := metadata[strings.TrimSpace(reference)]; item != nil {
				prepared.request.Image.Images[index] = item.dataURL
			}
		}
		if item := metadata[strings.TrimSpace(prepared.request.Image.MaskURL)]; item != nil {
			prepared.request.Image.MaskURL = item.dataURL
		}
	}
	for messageIndex := range prepared.request.Messages {
		for partIndex := range prepared.request.Messages[messageIndex].Parts {
			part := &prepared.request.Messages[messageIndex].Parts[partIndex]
			if item := metadata[strings.TrimSpace(part.ImageURL)]; item != nil {
				part.ImageURL = item.dataURL
			}
		}
	}
}
