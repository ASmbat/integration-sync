// Command server runs the integration-sync status API: a small framework
// for scheduling third-party syncs and exposing their health over HTTP.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
	"github.com/ASmbat/integration-sync/internal/handler"
	"github.com/ASmbat/integration-sync/internal/scheduler"
	"github.com/ASmbat/integration-sync/internal/store"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	statusStore := store.NewMemoryStatusStore()
	sched := scheduler.New(statusStore)

	// Register connectors. Adding a new vendor is exactly this one line
	// plus its Connector implementation — nothing else in the system needs
	// to change.
	ghUser := os.Getenv("GITHUB_USERNAME")
	if ghUser == "" {
		ghUser = "ASmbat"
	}
	sched.Register(connector.NewGitHubConnector(ghUser, os.Getenv("GITHUB_TOKEN")), 5*time.Minute)
	sched.Register(connector.NewMockConnector("billing-vendor-demo", 4), 30*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go sched.Start(ctx)

	h := handler.New(statusStore, sched)
	srv := &http.Server{
		Addr:         addr,
		Handler:      h.Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("graceful shutdown failed: %v", err)
	}
	log.Println("shutdown complete")
}
