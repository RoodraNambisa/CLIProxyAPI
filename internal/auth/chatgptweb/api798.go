package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const api798ResponseLimit int64 = 1 << 20

var api798VerificationCodePattern = regexp.MustCompile(`(^|[^0-9])([0-9]{6})([^0-9]|$)`)

type api798Message struct {
	Available       bool
	Code            string
	Subject         string
	Body            string
	DateRaw         string
	Date            time.Time
	HasReliableDate bool
	Fingerprint     string
}

type api798MailboxSession struct {
	service             *Service
	requestURL          string
	baselineFingerprint string
}

func newAPI798HTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxIdleConnsPerHost = 16
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (service *Service) newAPI798MailboxSession(credential *Credential) (*api798MailboxSession, *AuthError) {
	if credential == nil || strings.TrimSpace(credential.API798URL) == "" {
		return nil, api798AuthError("api798_url_missing", http.StatusBadRequest, false, true, "api798_poll", nil)
	}
	requestURL, errNormalize := normalizeAPI798RequestURL(credential.API798URL, credential.Email)
	if errNormalize != nil {
		return nil, api798AuthError("api798_url_invalid", http.StatusBadRequest, false, true, "api798_poll", errNormalize)
	}
	return &api798MailboxSession{service: service, requestURL: requestURL}, nil
}

func (session *api798MailboxSession) prepare(ctx context.Context) *AuthError {
	temporaryFailures := 0
	for {
		message, authError := session.fetch(ctx)
		if authError == nil {
			session.baselineFingerprint = message.Fingerprint
			return nil
		}
		if !authError.Retryable {
			return authError
		}
		temporaryFailures++
		if waitError := waitAPI798Retry(ctx, api798RetryDelay(
			session.service.options.API798PollInterval,
			temporaryFailures,
			authError.RetryAfter,
		)); waitError != nil {
			return waitError
		}
	}
}

func (session *api798MailboxSession) waitForCode(ctx context.Context, issuedAt time.Time) (string, *AuthError) {
	temporaryFailures := 0
	for {
		message, authError := session.fetch(ctx)
		if authError != nil {
			if !authError.Retryable {
				return "", authError
			}
			temporaryFailures++
		} else if session.isFresh(message, issuedAt, session.service.options.Now()) {
			return strings.TrimSpace(message.Code), nil
		} else {
			temporaryFailures = 0
		}

		retryAfter := time.Duration(0)
		if authError != nil {
			retryAfter = authError.RetryAfter
		}
		if waitError := waitAPI798Retry(ctx, api798RetryDelay(session.service.options.API798PollInterval, temporaryFailures, retryAfter)); waitError != nil {
			return "", waitError
		}
	}
}

func waitAPI798Retry(ctx context.Context, delay time.Duration) *AuthError {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return api798AuthError("api798_timeout", 0, true, false, "api798_poll", ctx.Err())
	case <-timer.C:
		return nil
	}
}

func api798RetryDelay(base time.Duration, temporaryFailures int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > DefaultAPI798AcquisitionTimeout {
			return DefaultAPI798AcquisitionTimeout
		}
		return retryAfter
	}
	if base <= 0 {
		base = 2 * time.Second
	}
	if base > DefaultAPI798RetryMaxDelay {
		base = DefaultAPI798RetryMaxDelay
	}
	if temporaryFailures <= 1 {
		return base
	}
	delay := base
	for attempt := 1; attempt < temporaryFailures; attempt++ {
		if delay >= DefaultAPI798RetryMaxDelay/2 {
			return DefaultAPI798RetryMaxDelay
		}
		delay *= 2
	}
	if delay > DefaultAPI798RetryMaxDelay {
		return DefaultAPI798RetryMaxDelay
	}
	return delay
}

func (session *api798MailboxSession) isFresh(message api798Message, issuedAt, now time.Time) bool {
	if !message.Available || strings.TrimSpace(message.Code) == "" || message.Fingerprint == "" || message.Fingerprint == session.baselineFingerprint {
		return false
	}
	if message.HasReliableDate {
		return !message.Date.Before(issuedAt.Truncate(time.Second))
	}
	return now.Sub(issuedAt) >= session.service.options.API798UndatedDelay
}

func (session *api798MailboxSession) fetch(ctx context.Context) (api798Message, *AuthError) {
	requestCtx, cancel := context.WithTimeout(ctx, session.service.options.API798RequestTimeout)
	defer cancel()
	request, errRequest := http.NewRequestWithContext(requestCtx, http.MethodGet, session.requestURL, nil)
	if errRequest != nil {
		return api798Message{}, api798AuthError(
			"api798_url_invalid",
			http.StatusBadRequest,
			false,
			true,
			"api798_poll",
			errors.New("construct API798 request"),
		)
	}
	request.Header.Set("accept", "application/json")
	response, errDo := session.service.options.API798HTTPClient.Do(request)
	if errDo != nil {
		if errors.Is(errDo, context.Canceled) && ctx.Err() != nil {
			return api798Message{}, api798AuthError("api798_timeout", 0, true, false, "api798_poll", safeAPI798NetworkCause(ctx.Err()))
		}
		if errors.Is(errDo, context.DeadlineExceeded) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return api798Message{}, api798AuthError("api798_request_timeout", 0, true, false, "api798_poll", safeAPI798NetworkCause(errDo))
		}
		return api798Message{}, api798AuthError("api798_network_error", 0, true, false, "api798_poll", safeAPI798NetworkCause(errDo))
	}
	defer func() { _ = response.Body.Close() }()

	payload, errRead := io.ReadAll(io.LimitReader(response.Body, api798ResponseLimit+1))
	if errRead != nil {
		return api798Message{}, api798AuthError("api798_network_error", response.StatusCode, true, false, "api798_poll", errRead)
	}
	if int64(len(payload)) > api798ResponseLimit {
		return api798Message{}, api798AuthError("api798_response_too_large", response.StatusCode, false, true, "api798_poll", nil)
	}

	switch {
	case response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden:
		return api798Message{}, api798AuthError("api798_authorization_failed", response.StatusCode, false, true, "api798_poll", nil)
	case response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError:
		authError := api798AuthError("api798_unavailable", response.StatusCode, true, false, "api798_poll", nil)
		authError.RetryAfter = parseAPI798RetryAfter(response.Header.Get("Retry-After"), session.service.options.Now())
		return api798Message{}, authError
	case response.StatusCode >= http.StatusBadRequest:
		return api798Message{}, api798AuthError("api798_request_rejected", response.StatusCode, false, true, "api798_poll", nil)
	}
	contentType := strings.ToLower(strings.TrimSpace(response.Header.Get("Content-Type")))
	if contentType == "" || !strings.Contains(contentType, "json") {
		return api798Message{}, api798AuthError("api798_response_invalid", response.StatusCode, false, true, "api798_poll", nil)
	}

	message, errDecode := decodeAPI798Message(payload)
	if errDecode != nil {
		return api798Message{}, api798AuthError("api798_response_invalid", response.StatusCode, false, true, "api798_poll", errDecode)
	}
	return message, nil
}

func parseAPI798RetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, errSeconds := strconv.ParseInt(value, 10, 64); errSeconds == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= int64(DefaultAPI798AcquisitionTimeout/time.Second) {
			return DefaultAPI798AcquisitionTimeout
		}
		return time.Duration(seconds) * time.Second
	}
	deadline, errDate := http.ParseTime(value)
	if errDate != nil || !deadline.After(now) {
		return 0
	}
	delay := deadline.Sub(now)
	if delay > DefaultAPI798AcquisitionTimeout {
		return DefaultAPI798AcquisitionTimeout
	}
	return delay
}

func safeAPI798NetworkCause(err error) error {
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("API798 request failed")
}

func api798AuthError(code string, status int, retryable, terminal bool, stage string, cause error) *AuthError {
	authError := newAuthError(code, LifecycleReauthRequired, status, retryable, terminal, "API798 email OTP authentication failed", cause)
	if retryable {
		authError.State = LifecycleLoginPending
		authError.LifecycleState = LifecycleLoginPending
	}
	authError.FailureStage = stage
	return authError
}

func decodeAPI798Message(payload []byte) (api798Message, error) {
	var root any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if errDecode := decoder.Decode(&root); errDecode != nil {
		return api798Message{}, fmt.Errorf("decode API798 JSON: %w", errDecode)
	}
	var trailing any
	if errTrailing := decoder.Decode(&trailing); !errors.Is(errTrailing, io.EOF) {
		if errTrailing == nil {
			return api798Message{}, errors.New("API798 JSON must contain exactly one value")
		}
		return api798Message{}, fmt.Errorf("decode trailing API798 JSON: %w", errTrailing)
	}
	rootMap, ok := root.(map[string]any)
	if !ok {
		return api798Message{}, errors.New("API798 JSON root must be an object")
	}
	if success, found := api798BoolField(rootMap, "success"); found && !success {
		return api798Message{}, nil
	}
	data := rootMap
	for _, key := range []string{"data", "result", "mail", "message"} {
		if nested, okNested := rootMap[key].(map[string]any); okNested {
			data = nested
			break
		}
	}
	message := api798Message{
		Code:    api798StringField(data, "code", "otp", "verification_code"),
		Subject: api798StringField(data, "subject", "title"),
		Body:    api798StringField(data, "body", "content", "html", "text"),
		DateRaw: api798StringField(data, "date", "received_at", "created_at", "timestamp"),
	}
	if message.Code == "" {
		message.Code = api798StringField(rootMap, "code", "otp", "verification_code")
	}
	if message.Subject == "" {
		message.Subject = api798StringField(rootMap, "subject", "title")
	}
	if message.Body == "" {
		message.Body = api798StringField(rootMap, "body", "content", "html", "text")
	}
	if message.DateRaw == "" {
		message.DateRaw = api798StringField(rootMap, "date", "received_at", "created_at", "timestamp")
	}
	message.Available = strings.TrimSpace(message.Code+message.Subject+message.Body) != ""
	if message.Code == "" {
		message.Code = extractAPI798VerificationCode(message.Subject + "\n" + message.Body)
	}
	message.Date, message.HasReliableDate = parseAPI798Date(message.DateRaw)
	message.Fingerprint = api798MessageFingerprint(message)
	return message, nil
}

func extractAPI798VerificationCode(value string) string {
	match := api798VerificationCodePattern.FindStringSubmatch(value)
	if len(match) < 3 {
		return ""
	}
	return match[2]
}

func api798BoolField(values map[string]any, key string) (bool, bool) {
	value, ok := values[key]
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		parsed, errParse := strconv.ParseBool(strings.TrimSpace(typed))
		return parsed, errParse == nil
	case json.Number:
		parsed, errParse := strconv.ParseInt(typed.String(), 10, 64)
		return parsed != 0, errParse == nil
	default:
		return false, false
	}
}

func api798StringField(values map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := values[key]
		if !ok || value == nil {
			continue
		}
		switch typed := value.(type) {
		case string:
			if result := strings.TrimSpace(typed); result != "" {
				return result
			}
		case json.Number:
			return typed.String()
		}
	}
	return ""
}

func parseAPI798Date(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if unixValue, errParse := strconv.ParseInt(raw, 10, 64); errParse == nil {
		if unixValue > 1_000_000_000_000 {
			unixValue /= 1000
		}
		if unixValue > 0 {
			return time.Unix(unixValue, 0), true
		}
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		time.RFC1123Z,
		time.RFC1123,
		"2006-01-02 15:04:05 -0700",
	} {
		if parsed, errParse := time.Parse(layout, raw); errParse == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func api798MessageFingerprint(message api798Message) string {
	if !message.Available {
		return ""
	}
	normalize := func(value string) string {
		return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	}
	material := strings.Join([]string{
		normalize(message.DateRaw),
		normalize(message.Code),
		normalize(message.Subject),
		normalize(message.Body),
	}, "\x00")
	sum := sha256.Sum256([]byte(material))
	return hex.EncodeToString(sum[:])
}
