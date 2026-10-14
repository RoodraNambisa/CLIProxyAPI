package xai

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	log "github.com/sirupsen/logrus"
)

const FallbackClientVersion = "1.0.52"

var clientVersion = struct {
	sync.RWMutex
	value string
}{value: FallbackClientVersion}

// ClientVersion is shared by OAuth, resource queries and inference.
func ClientVersion() string {
	clientVersion.RLock()
	defer clientVersion.RUnlock()
	return clientVersion.value
}

func ValidClientVersion(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	numbers := [3]int{}
	for i, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return false
		}
		for _, c := range part {
			if c < '0' || c > '9' {
				return false
			}
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return false
		}
		numbers[i] = n
	}
	return numbers[0] > 1 || (numbers[0] == 1 && (numbers[1] > 0 || numbers[2] >= 13))
}

// RunClientVersionUpdater uses the service lifetime, without transport or body deadlines.
func RunClientVersionUpdater(ctx context.Context, proxyURL func() string) {
	ticker := time.NewTicker(3 * time.Hour)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		sdkCfg := config.SDKConfig{ProxyURL: proxyURL()}
		client := util.SetProxy(&sdkCfg, &http.Client{})
		version, err := FetchClientVersion(ctx, client, "https://registry.npmjs.org/@xai-official/grok/latest")
		if err == nil {
			clientVersion.Lock()
			clientVersion.value = version
			clientVersion.Unlock()
		} else if ctx.Err() == nil {
			log.WithError(err).Warn("Grok version refresh failed; keeping cached version")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func FetchClientVersion(ctx context.Context, client *http.Client, endpoint string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.WithError(err).Debug("close Grok version response")
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("Grok version registry returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil {
		return "", err
	}
	if len(data) > 1<<20 {
		return "", fmt.Errorf("Grok version response exceeds 1 MiB")
	}
	var info struct {
		Version string `json:"version"`
	}
	if err = json.Unmarshal(data, &info); err != nil {
		return "", err
	}
	if !ValidClientVersion(info.Version) {
		return "", fmt.Errorf("invalid stable Grok version")
	}
	return info.Version, nil
}
