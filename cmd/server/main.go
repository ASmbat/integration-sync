// Command server runs the integration-sync status API: a small framework
// for scheduling third-party syncs and exposing their health over HTTP.
package main

import (
	"context"
	"errors"
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

	// Adding a vendor = a Connector implementation plus one Register call.
	ghUser := os.Getenv("GITHUB_USERNAME")
	if ghUser == "" {
		ghUser = "ASmbat"
	}
	if err := sched.Register(connector.NewGitHubConnector(ghUser, os.Getenv("GITHUB_TOKEN")), 5*time.Minute); err != nil {
		log.Fatal(err)
	}
	if err := sched.Register(connector.NewMockConnector("billing-vendor-demo", 4), 30*time.Second); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	schedDone := make(chan struct{})
	go func() {
		if err := sched.Start(ctx); err != nil {
			log.Printf("scheduler: %v", err)
		}
		close(schedDone)
	}()

	h := handler.New(statusStore, sched)
	srv := &http.Server{
		Addr:         addr,
		Handler:      h.Routes(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: handler.SyncTimeout + 5*time.Second, // POST /sync can take up to SyncTimeout
	}

	go func() {
		log.Printf("listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	select {
	case <-schedDone: // in-flight syncs finished
	case <-shutdownCtx.Done():
		log.Println("timed out waiting for running syncs")
	}
	log.Println("shutdown complete")
}
