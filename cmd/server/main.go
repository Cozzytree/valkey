package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/server"
)

const version = "0.0.1"

const banner = `
 __   __    _  _    _  _  ____ _  _
 \ \ / /_ _| || |__| || ||  __| || |
  \ V / _' | || / / | || || _| | || |
   \_/\__,_|_||_\_\_|__  ||____|__  |
                      |___|      |___| %s

`

func main() {
	args := os.Args[1:]

	switch {
	case len(args) > 0 && (args[0] == "-h" || args[0] == "--help" || args[0] == "help"):
		fmt.Print(config.Help())
		os.Exit(0)
	case len(args) > 0 && (args[0] == "--version" || args[0] == "-v" || args[0] == "version"):
		fmt.Printf("Valkey server v%s\n", version)
		os.Exit(0)
	}

	// ── Parse CLI args ────────────────────────────────────────────────────────
	parsed, err := config.Parse(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n\nRun with --help for usage.\n", err)
		os.Exit(1)
	}

	// ── Build config: defaults → config file (TODO) → CLI overrides ──────────
	cfg := config.DefaultConfig()
	if err := parsed.Apply(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		os.Exit(1)
	}

	// ── Logging ───────────────────────────────────────────────────────────────
	logger := log.New(os.Stdout, "", log.LstdFlags)

	if cfg.General.AlwaysShowLogo {
		fmt.Printf(banner, version)
	}
	if parsed.ConfigFile != "" {
		logger.Printf("* Config file: %s", parsed.ConfigFile)
	}
	logger.Printf("* Log level:   %s", cfg.General.LogLevel)
	logger.Printf("* Databases:   %d", cfg.General.Databases)
	logger.Printf("* Read buffer: %d bytes / conn", cfg.Network.ReadBufSize)

	// ── Start server ──────────────────────────────────────────────────────────
	srv := server.New(cfg, logger)
	if err := srv.Start(); err != nil {
		logger.Fatalf("failed to start: %v", err)
	}

	// ── Wait for SIGINT / SIGTERM ─────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Printf("* Received signal %s", sig)

	srv.Stop()
}
