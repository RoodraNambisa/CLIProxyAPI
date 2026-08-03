package chatgptweb

import "time"

const AccountInfoDiagnosticsCapacity = 100

// AccountInfoDiagnosticEvent contains only the safe structure extracted from
// one failed account-info request. It is never persisted.
type AccountInfoDiagnosticEvent struct {
	OccurredAt               time.Time
	Phase                    string
	Stage                    string
	Reason                   string
	ErrorType                string
	HTTPStatus               int
	ContentType              string
	Cloudflare               bool
	BodyKind                 string
	AccountsKind             string
	LimitsProgressKind       string
	LimitsProgressCount      int
	ImageQuotaFeaturePresent *bool
	ImageQuotaRemainingKind  string
	ImageQuotaRemaining      *int64
	ImageQuotaResetAfter     string
	ErrorEnvelopeKind        string
	ResponseBytes            int
	ContentLength            int64
	UpstreamErrorCode        string
	AuthIndex                string
	Attempt                  int
}

// AccountInfoDiagnosticRecord is one de-duplicated, management-safe error.
type AccountInfoDiagnosticRecord struct {
	ID                       string    `json:"id"`
	Phase                    string    `json:"phase"`
	Stage                    string    `json:"stage"`
	Reason                   string    `json:"reason"`
	ErrorType                string    `json:"error_type,omitempty"`
	HTTPStatus               int       `json:"http_status,omitempty"`
	ContentType              string    `json:"content_type,omitempty"`
	Cloudflare               bool      `json:"cloudflare"`
	BodyKind                 string    `json:"body_kind,omitempty"`
	AccountsKind             string    `json:"accounts_kind,omitempty"`
	LimitsProgressKind       string    `json:"limits_progress_kind,omitempty"`
	LimitsProgressCount      int       `json:"limits_progress_count,omitempty"`
	ImageQuotaFeaturePresent *bool     `json:"image_quota_feature_present,omitempty"`
	ImageQuotaRemainingKind  string    `json:"image_quota_remaining_kind,omitempty"`
	LastRemaining            *int64    `json:"last_remaining,omitempty"`
	MinRemaining             *int64    `json:"min_remaining,omitempty"`
	MaxRemaining             *int64    `json:"max_remaining,omitempty"`
	ImageQuotaResetAfter     string    `json:"image_quota_reset_after,omitempty"`
	ErrorEnvelopeKind        string    `json:"error_envelope_kind,omitempty"`
	ResponseBytes            int       `json:"response_bytes"`
	ContentLength            int64     `json:"content_length"`
	UpstreamErrorCode        string    `json:"upstream_error_code,omitempty"`
	Count                    uint64    `json:"count"`
	FirstSeen                time.Time `json:"first_seen"`
	LastSeen                 time.Time `json:"last_seen"`
	LastAuthIndex            string    `json:"last_auth_index,omitempty"`
	LastAttempt              int       `json:"last_attempt,omitempty"`
}

// AccountInfoDiagnosticsSnapshot is an in-memory diagnostics snapshot.
type AccountInfoDiagnosticsSnapshot struct {
	Enabled      bool                          `json:"enabled"`
	Capacity     int                           `json:"capacity"`
	UniqueCount  int                           `json:"unique_count"`
	TotalCount   uint64                        `json:"total_count"`
	EvictedCount uint64                        `json:"evicted_count"`
	Records      []AccountInfoDiagnosticRecord `json:"records"`
}
