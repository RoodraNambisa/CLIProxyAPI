package management

import (
	"errors"
	"strings"

	"golang.org/x/net/http/httpguts"
)

func validateUserAgentHeader(headers map[string]string) error {
	for name, value := range headers {
		if strings.EqualFold(strings.TrimSpace(name), "User-Agent") && !httpguts.ValidHeaderFieldValue(value) {
			return errors.New("User-Agent contains invalid HTTP header characters")
		}
	}
	return nil
}
