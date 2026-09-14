package main

import (
	"context"
	"crypto/sha256"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementasset"
	log "github.com/sirupsen/logrus"
)

func runSentinelSolverOnly(path, password string) error {
	path, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		return err
	}
	server, err := api.NewSentinelOnlyServer(cfg, path, password)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Duration(cfg.SentinelSolver.Limits().DrainTimeoutSeconds+5)*time.Second)
		defer stopCancel()
		if errStop := server.Stop(stopCtx); errStop != nil {
			log.WithError(errStop).Warn("solver shutdown failed")
		}
	}()
	address, serveErr, err := server.StartListening()
	if err != nil {
		return err
	}
	log.WithField("management_address", address.String()).Info("Sentinel-only instance started")
	managementasset.StartAutoUpdater(ctx, path)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = watcher.Close() }()
	if err = watcher.Add(filepath.Dir(path)); err != nil {
		return err
	}
	body, _ := os.ReadFile(path)
	lastHash := sha256.Sum256(body)
	for {
		select {
		case <-ctx.Done():
			return nil
		case errServe := <-serveErr:
			return errServe
		case errWatch, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.WithError(errWatch).Warn("solver config watcher error")
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if filepath.Clean(event.Name) != path || event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
				continue
			}
			body, errRead := os.ReadFile(path)
			if errRead != nil {
				continue
			}
			hash := sha256.Sum256(body)
			if hash == lastHash {
				continue
			}
			candidate, errLoad := config.LoadConfig(path)
			if errLoad != nil {
				log.Warn("solver configuration reload rejected")
				continue
			}
			if errUpdate := server.UpdateClients(candidate); errUpdate != nil {
				log.WithError(errUpdate).Warn("solver configuration reload rejected")
				continue
			}
			lastHash = hash
		}
	}
}
