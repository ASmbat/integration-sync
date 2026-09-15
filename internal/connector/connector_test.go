package connector

import (
	"context"
	"testing"
)

func TestMockConnector_SucceedsThenFailsOnCadence(t *testing.T) {
	c := NewMockConnector("test", 3)
	ctx := context.Background()

	for i := 1; i <= 6; i++ {
		res, err := c.Sync(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		wantRateLimited := i%3 == 0
		gotRateLimited := res.Status == StatusRateLimited
		if wantRateLimited != gotRateLimited {
			t.Errorf("call %d: expected rate-limited=%v, got status=%s", i, wantRateLimited, res.Status)
		}
	}
}

func TestMockConnector_NoFailuresWhenDisabled(t *testing.T) {
	c := NewMockConnector("test", 0)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		res, err := c.Sync(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res.Status != StatusOK {
			t.Errorf("expected StatusOK, got %s", res.Status)
		}
	}
}

func TestMockConnector_IDAndDisplayName(t *testing.T) {
	c := NewMockConnector("billing-api", 0)
	if c.ID() != "billing-api" {
		t.Errorf("unexpected ID: %s", c.ID())
	}
	if c.DisplayName() != "billing-api" {
		t.Errorf("unexpected DisplayName: %s", c.DisplayName())
	}
}
