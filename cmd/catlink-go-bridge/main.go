package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/129tyc/catlink-stream-bridge/internal/bridge"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	configPath := os.Getenv("CATLINK_GO_CONFIG_FILE")
	if configPath == "" && len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	if configPath == "" {
		logger.Error("config file is required")
		os.Exit(2)
	}
	file, err := os.Open(configPath)
	if err != nil {
		logger.Error("open config", "error", err)
		os.Exit(2)
	}
	cfg, err := bridge.LoadConfig(file)
	_ = file.Close()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(2)
	}

	accounts := make(map[string]*bridge.AccountTokenManager, len(cfg.Accounts))
	client := &http.Client{Timeout: 20 * time.Second}
	for _, account := range cfg.Accounts {
		accounts[account.ID] = bridge.NewAccountTokenManager(account, client)
	}
	devices := make(map[string]bridge.DeviceConfig, len(cfg.Devices))
	for _, device := range cfg.Devices {
		devices[device.ID] = device
	}
	routeNames := make([]string, 0, len(cfg.Routes))
	for _, route := range cfg.Routes {
		routeNames = append(routeNames, route.Name)
	}
	health := bridge.NewHealth(routeNames)
	publisher := bridge.NewRTSPPublisher(cfg.Listen, bridge.NewC07TalkTransportFactory(client, cfg.OpenDomain))
	if err := publisher.Start(); err != nil {
		logger.Error("start RTSP server", "error", err)
		os.Exit(1)
	}
	health.SetRTSP(true)
	defer publisher.Shutdown()

	healthServer := &http.Server{Addr: cfg.HealthListen, Handler: health}
	go func() {
		if err := healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var waitGroup sync.WaitGroup
	for _, route := range cfg.Routes {
		device := devices[route.DeviceID]
		account := accounts[device.AccountID]
		runner := bridge.NewRouteRunner(route, device, account, cfg.OpenDomain, client, publisher, health, logger)
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			runner.Run(ctx)
		}()
	}

	<-ctx.Done()
	_ = healthServer.Shutdown(context.Background())
	waitGroup.Wait()
}
