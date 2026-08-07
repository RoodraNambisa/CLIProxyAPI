package management

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func TestHandlerConfigSnapshotIsolatedFromSetConfigInput(t *testing.T) {
	handler := NewHandler(&config.Config{}, "", nil)
	candidate := &config.Config{Debug: true}
	if errSet := handler.SetConfig(candidate); errSet != nil {
		t.Fatalf("SetConfig() error = %v", errSet)
	}
	candidate.Debug = false
	if snapshot := handler.currentConfig(); snapshot == nil || !snapshot.Debug {
		t.Fatalf("published snapshot changed with caller-owned config: %#v", snapshot)
	}
}

func TestHandlerConfigAndManagementAccessSnapshotsAreRaceSafe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	secretA, errHashA := bcrypt.GenerateFromPassword([]byte("secret-a"), bcrypt.MinCost)
	if errHashA != nil {
		t.Fatalf("hash secret A: %v", errHashA)
	}
	secretB, errHashB := bcrypt.GenerateFromPassword([]byte("secret-b"), bcrypt.MinCost)
	if errHashB != nil {
		t.Fatalf("hash secret B: %v", errHashB)
	}
	handler := NewHandler(&config.Config{RemoteManagement: config.RemoteManagement{
		AllowRemote: true,
		SecretKey:   string(secretA),
	}}, "", nil)
	router := gin.New()
	router.Use(handler.Middleware())
	router.GET("/management", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	const iterations = 100
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			secret := secretA
			password := "secret-a"
			if iteration%2 == 1 {
				secret = secretB
				password = "secret-b"
			}
			if errSet := handler.SetConfig(&config.Config{
				Debug: iteration%2 == 0,
				RemoteManagement: config.RemoteManagement{
					AllowRemote: true,
					SecretKey:   string(secret),
				},
			}); errSet != nil {
				t.Errorf("SetConfig() error = %v", errSet)
				return
			}
			handler.SetLocalPassword(password)
		}
	}()
	for reader := 0; reader < 4; reader++ {
		wait.Add(1)
		go func(reader int) {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				if snapshot := handler.currentConfig(); snapshot == nil {
					t.Error("currentConfig() returned nil")
					return
				}
				request := httptest.NewRequest(http.MethodGet, "/management", nil)
				request.RemoteAddr = "127.0.0.1:12345"
				if (reader+iteration)%2 == 0 {
					request.Header.Set("Authorization", "Bearer secret-a")
				} else {
					request.Header.Set("Authorization", "Bearer secret-b")
				}
				recorder := httptest.NewRecorder()
				router.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusNoContent && recorder.Code != http.StatusUnauthorized {
					t.Errorf("management response status = %d", recorder.Code)
					return
				}
			}
		}(reader)
	}
	wait.Wait()
}
