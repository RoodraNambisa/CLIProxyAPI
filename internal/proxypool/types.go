package proxypool

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const bindingStateVersion = 1

// Binding stores the stable logical node selected for one credential.
type Binding struct {
	ID                string    `json:"id"`
	AuthID            string    `json:"auth_id"`
	Pool              string    `json:"pool"`
	Entry             string    `json:"entry"`
	Port              int       `json:"port,omitempty"`
	PlaceholderValues []string  `json:"placeholder_values,omitempty"`
	BoundAt           time.Time `json:"bound_at"`
}

type bindingStateFile struct {
	Version  int                `json:"version"`
	Bindings map[string]Binding `json:"bindings"`
}

// TraceResult is the health information returned by one proxy probe.
type TraceResult struct {
	OK        bool      `json:"ok"`
	IP        string    `json:"ip,omitempty"`
	Location  string    `json:"loc,omitempty"`
	HTTP      string    `json:"http,omitempty"`
	TLS       string    `json:"tls,omitempty"`
	Colo      string    `json:"colo,omitempty"`
	ElapsedMS int64     `json:"elapsed_ms,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
	Error     string    `json:"error,omitempty"`
	Message   string    `json:"message,omitempty"`
}

type nodeHealth struct {
	TraceResult
	Pool       string
	Entry      string
	BindingID  string
	MaskedURL  string
	RetryAfter time.Time
	Generation uint64
	ProbeEpoch uint64
}

// BindingStatus is the management-safe view of one persisted binding.
type BindingStatus struct {
	AuthID       string     `json:"auth_id"`
	AuthIndex    string     `json:"auth_index,omitempty"`
	Provider     string     `json:"provider,omitempty"`
	Pool         string     `json:"pool"`
	Entry        string     `json:"entry"`
	Port         int        `json:"port,omitempty"`
	BindingID    string     `json:"binding_id"`
	ProxyURL     string     `json:"proxy_url"`
	BoundAt      time.Time  `json:"bound_at"`
	Healthy      *bool      `json:"healthy"`
	LastCheckAt  *time.Time `json:"last_check_at,omitempty"`
	NextCheckAt  *time.Time `json:"next_check_at,omitempty"`
	IP           string     `json:"ip,omitempty"`
	Location     string     `json:"loc,omitempty"`
	ElapsedMS    int64      `json:"elapsed_ms,omitempty"`
	Error        string     `json:"error,omitempty"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// PoolStatus summarizes runtime health without exposing proxy credentials.
type PoolStatus struct {
	Name           string          `json:"name"`
	BindingCount   int             `json:"binding_count"`
	HealthyCount   int             `json:"healthy_count"`
	UnhealthyCount int             `json:"unhealthy_count"`
	UnknownCount   int             `json:"unknown_count"`
	LastCheckAt    *time.Time      `json:"last_check_at,omitempty"`
	Bindings       []BindingStatus `json:"bindings,omitempty"`
}

// CheckResult is one management-triggered bound or sampled node check.
type CheckResult struct {
	Pool      string    `json:"pool"`
	Entry     string    `json:"entry"`
	Port      int       `json:"port,omitempty"`
	BindingID string    `json:"binding_id,omitempty"`
	Bound     bool      `json:"bound"`
	ProxyURL  string    `json:"proxy_url"`
	OK        bool      `json:"ok"`
	IP        string    `json:"ip,omitempty"`
	Location  string    `json:"loc,omitempty"`
	HTTP      string    `json:"http,omitempty"`
	TLS       string    `json:"tls,omitempty"`
	Colo      string    `json:"colo,omitempty"`
	ElapsedMS int64     `json:"elapsed_ms,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
	Error     string    `json:"error,omitempty"`
	Message   string    `json:"message,omitempty"`
}

// RebindResult reports one requested credential rebind.
type RebindResult struct {
	AuthID     string         `json:"auth_id"`
	AuthIndex  string         `json:"auth_index,omitempty"`
	Updated    bool           `json:"updated"`
	Binding    *BindingStatus `json:"binding,omitempty"`
	Error      string         `json:"error,omitempty"`
	HTTPStatus int            `json:"status,omitempty"`
}

// AuthSource supplies current runtime credentials to management and cleanup.
type AuthSource interface {
	List() []*coreauth.Auth
	GetByID(id string) (*coreauth.Auth, bool)
}

// UnavailableError reports a strict matched-pool failure.
type UnavailableError struct {
	Pool      string
	RetryTime time.Time
	Cause     error
}

func (e *UnavailableError) Error() string {
	message := e.Message()
	var body struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	body.Error.Message = message
	body.Error.Type = "server_error"
	body.Error.Code = "proxy_unavailable"
	payload, errMarshal := json.Marshal(body)
	if errMarshal != nil {
		return message
	}
	return string(payload)
}

// Message returns the management-safe human description of the pool failure.
func (e *UnavailableError) Message() string {
	if e == nil {
		return "no proxy node is available"
	}
	if e.Pool == "" {
		return "no proxy node is available"
	}
	return fmt.Sprintf("no proxy node is available in pool %s", e.Pool)
}

func (e *UnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (*UnavailableError) StatusCode() int      { return http.StatusServiceUnavailable }
func (*UnavailableError) SkipAuthResult() bool { return true }
func (*UnavailableError) RetryOtherAuth() bool { return true }

func (e *UnavailableError) RetryAfter() *time.Duration {
	if e == nil || e.RetryTime.IsZero() {
		return nil
	}
	wait := time.Until(e.RetryTime)
	if wait < time.Second {
		wait = time.Second
	}
	return &wait
}

func (e *UnavailableError) Headers() http.Header {
	header := make(http.Header)
	if retryAfter := e.RetryAfter(); retryAfter != nil {
		seconds := int64((*retryAfter + time.Second - 1) / time.Second)
		header.Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	return header
}

type traceChecker func(context.Context, string) TraceResult
