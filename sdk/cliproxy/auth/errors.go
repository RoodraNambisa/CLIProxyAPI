package auth

// Error describes an authentication related failure in a provider agnostic format.
type Error struct {
	// Code is a short machine readable identifier.
	Code string `json:"code,omitempty"`
	// Message is a human readable description of the failure.
	Message string `json:"message"`
	// Retryable indicates whether a retry might fix the issue automatically.
	Retryable bool `json:"retryable"`
	// HTTPStatus optionally records an HTTP-like status code for the error.
	HTTPStatus int `json:"http_status,omitempty"`
	// Diagnostic contains provider-supplied troubleshooting metadata for authenticated management views.
	Diagnostic *ErrorDiagnostic `json:"diagnostic,omitempty"`
}

// ErrorDiagnostic contains bounded troubleshooting metadata for authenticated management views.
type ErrorDiagnostic struct {
	Provider             string `json:"provider,omitempty"`
	AuthIndex            string `json:"auth_index,omitempty"`
	Stage                string `json:"stage,omitempty"`
	Code                 string `json:"code,omitempty"`
	ResponseType         string `json:"response_type,omitempty"`
	ContentType          string `json:"content_type,omitempty"`
	CFRay                string `json:"cf_ray,omitempty"`
	TargetHost           string `json:"target_host,omitempty"`
	TargetPath           string `json:"target_path,omitempty"`
	Persona              string `json:"persona,omitempty"`
	CatalogVersion       string `json:"catalog_version,omitempty"`
	CatalogID            string `json:"catalog_id,omitempty"`
	TransportPersonaID   string `json:"transport_persona_id,omitempty"`
	BrowserEnvironmentID string `json:"browser_environment_id,omitempty"`
	TLSProfile           string `json:"tls_profile,omitempty"`
	UAMajor              string `json:"ua_major,omitempty"`
	Platform             string `json:"platform,omitempty"`
	ResponseBytes        int64  `json:"response_bytes,omitempty"`
	ResponseBody         string `json:"response_body,omitempty"`
	// ResponseBodyTruncated reports that ResponseBody contains only the bounded prefix.
	ResponseBodyTruncated bool `json:"response_body_truncated,omitempty"`
	Attempts              int  `json:"attempts,omitempty"`
	HTTPStatus            int  `json:"http_status,omitempty"`
	Cloudflare            bool `json:"cloudflare,omitempty"`
	Retryable             bool `json:"retryable"`
}

// Clone returns an independent diagnostic value.
func (diagnostic *ErrorDiagnostic) Clone() *ErrorDiagnostic {
	if diagnostic == nil {
		return nil
	}
	cloned := *diagnostic
	return &cloned
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

// StatusCode implements optional status accessor for manager decision making.
func (e *Error) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.HTTPStatus
}
