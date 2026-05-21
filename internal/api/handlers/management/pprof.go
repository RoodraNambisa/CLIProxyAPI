package management

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	runtimepprof "runtime/pprof"
	runtimetrace "runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const (
	defaultPprofProfileSeconds = 30
	defaultPprofTraceSeconds   = 5
	maxPprofSeconds            = 120
)

var (
	pprofLookPath = exec.LookPath
	pprofCommand  = exec.CommandContext
	pprofBinary   = os.Executable
)

// GetPprofConfig returns pprof runtime and visualization capabilities for the management UI.
func (h *Handler) GetPprofConfig(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config is not available"})
		return
	}
	_, goErr := pprofLookPath("go")
	_, dotErr := pprofLookPath("dot")
	c.JSON(http.StatusOK, gin.H{
		"pprof": gin.H{
			"enable": cfg.Pprof.Enable,
			"addr":   cfg.Pprof.Addr,
			"management": gin.H{
				"profiles":           []string{"profile", "heap", "allocs", "goroutine", "block", "mutex", "threadcreate", "trace"},
				"formats":            []string{"top", "svg", "proto", "text"},
				"go_tool_available":  goErr == nil,
				"graphviz_available": dotErr == nil,
				"max_seconds":        maxPprofSeconds,
			},
		},
	})
}

func (h *Handler) GetPprofEnable(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config is not available"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enable": cfg.Pprof.Enable})
}

func (h *Handler) PutPprofEnable(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.Pprof.Enable = v })
}

func (h *Handler) GetPprofAddr(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config is not available"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"addr": cfg.Pprof.Addr})
}

func (h *Handler) PutPprofAddr(c *gin.Context) {
	h.updateStringField(c, func(v string) { h.cfg.Pprof.Addr = strings.TrimSpace(v) })
}

// GetPprofProfile collects a profile and optionally renders it through go tool pprof.
func (h *Handler) GetPprofProfile(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config is not available"})
		return
	}
	if !cfg.Pprof.Enable {
		c.JSON(http.StatusForbidden, gin.H{"error": "pprof is disabled"})
		return
	}

	profileName := strings.ToLower(strings.TrimSpace(c.Param("profile")))
	if !isSupportedPprofProfile(profileName) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported pprof profile"})
		return
	}
	format := normalizePprofFormat(profileName, c.Query("format"))
	if format == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported pprof format"})
		return
	}

	switch format {
	case "proto":
		h.writeRawPprofProfile(c, profileName)
	case "text":
		h.writeTextPprofProfile(c, profileName)
	case "top", "svg":
		h.writeAnalyzedPprofProfile(c, profileName, format)
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported pprof format"})
	}
}

func (h *Handler) currentConfig() *config.Config {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		return nil
	}
	return h.cfg
}

func (h *Handler) writeRawPprofProfile(c *gin.Context, profileName string) {
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.pb.gz"`, profileName))
	if err := writePprofProfile(c.Request.Context(), c.Writer, profileName, pprofSeconds(c, profileName), 0); err != nil {
		writePprofCollectionError(c, err)
	}
}

func (h *Handler) writeTextPprofProfile(c *gin.Context, profileName string) {
	if profileName == "profile" || profileName == "trace" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "text format is not supported for this profile"})
		return
	}
	c.Header("Content-Type", "text/plain; charset=utf-8")
	if err := writePprofProfile(c.Request.Context(), c.Writer, profileName, pprofSeconds(c, profileName), 1); err != nil {
		writePprofCollectionError(c, err)
	}
}

func (h *Handler) writeAnalyzedPprofProfile(c *gin.Context, profileName string, format string) {
	if profileName == "trace" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "analysis is not supported for trace profiles"})
		return
	}
	if _, err := pprofLookPath("go"); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "go tool pprof is not available"})
		return
	}
	if format == "svg" {
		if _, err := pprofLookPath("dot"); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "graphviz dot is not available"})
			return
		}
	}

	tempDir, err := os.MkdirTemp("", "cli-proxy-pprof-*")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create temp dir: %v", err)})
		return
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	profilePath := filepath.Join(tempDir, profileName+".pb.gz")
	file, err := os.Create(profilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to create profile file: %v", err)})
		return
	}
	err = writePprofProfile(c.Request.Context(), file, profileName, pprofSeconds(c, profileName), 0)
	if closeErr := file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		writePprofCollectionError(c, err)
		return
	}

	binaryPath, err := pprofBinary()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to locate executable: %v", err)})
		return
	}
	output, err := runPprofTool(c.Request.Context(), format, binaryPath, profilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if format == "svg" {
		c.Header("Content-Type", "image/svg+xml; charset=utf-8")
	} else {
		c.Header("Content-Type", "text/plain; charset=utf-8")
	}
	_, _ = c.Writer.Write(output)
}

func writePprofProfile(ctx context.Context, w io.Writer, profileName string, seconds int, debug int) error {
	switch profileName {
	case "profile":
		return writeCPUProfile(ctx, w, seconds)
	case "trace":
		return writeTraceProfile(ctx, w, seconds)
	default:
		profile := runtimepprof.Lookup(profileName)
		if profile == nil {
			return fmt.Errorf("profile %q is not available", profileName)
		}
		return profile.WriteTo(w, debug)
	}
}

func writeCPUProfile(ctx context.Context, w io.Writer, seconds int) error {
	if err := runtimepprof.StartCPUProfile(w); err != nil {
		return err
	}
	defer runtimepprof.StopCPUProfile()
	return waitPprofDuration(ctx, seconds)
}

func writeTraceProfile(ctx context.Context, w io.Writer, seconds int) error {
	if err := runtimetrace.Start(w); err != nil {
		return err
	}
	defer runtimetrace.Stop()
	return waitPprofDuration(ctx, seconds)
}

func waitPprofDuration(ctx context.Context, seconds int) error {
	if seconds <= 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(seconds) * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func runPprofTool(ctx context.Context, format string, binaryPath string, profilePath string) ([]byte, error) {
	flag := "-" + format
	args := []string{"tool", "pprof", flag, "-nodecount=80", binaryPath, profilePath}
	cmd := pprofCommand(ctx, "go", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("go tool pprof failed: %s", msg)
	}
	return output, nil
}

func normalizePprofFormat(profileName string, raw string) string {
	format := strings.ToLower(strings.TrimSpace(raw))
	if format == "" {
		if profileName == "trace" {
			return "proto"
		}
		if profileName == "goroutine" {
			return "text"
		}
		return "top"
	}
	switch format {
	case "top", "svg", "proto":
		return format
	case "text":
		if profileName == "profile" || profileName == "trace" {
			return ""
		}
		return format
	default:
		return ""
	}
}

func pprofSeconds(c *gin.Context, profileName string) int {
	defaultSeconds := defaultPprofProfileSeconds
	if profileName == "trace" {
		defaultSeconds = defaultPprofTraceSeconds
	}
	raw := strings.TrimSpace(c.Query("seconds"))
	if raw == "" {
		return defaultSeconds
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultSeconds
	}
	if seconds > maxPprofSeconds {
		return maxPprofSeconds
	}
	return seconds
}

func isSupportedPprofProfile(profileName string) bool {
	switch profileName {
	case "profile", "heap", "allocs", "goroutine", "block", "mutex", "threadcreate", "trace":
		return true
	default:
		return false
	}
}

func writePprofCollectionError(c *gin.Context, err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to collect pprof profile: %v", err)})
}
