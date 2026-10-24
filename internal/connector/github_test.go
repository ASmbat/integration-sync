package connector

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestGitHub(t *testing.T, h http.HandlerFunc) *GitHubConnector {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c := NewGitHubConnector("octo cat", "secret-token")
	c.BaseURL = srv.URL
	return c
}

func reposJSON(n int) string {
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"name":"repo%d","updated_at":"2026-01-01T00:00:00Z"}`, i)
	}
	return "[" + strings.Join(items, ",") + "]"
}

func TestGitHub_SinglePage(t *testing.T) {
	c := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Errorf("missing auth header, got %q", got)
		}
		if r.URL.EscapedPath() != "/users/octo%20cat/repos" {
			t.Errorf("username not path-escaped: %q", r.URL.EscapedPath())
		}
		w.Header().Set("X-RateLimit-Remaining", "59")
		fmt.Fprint(w, reposJSON(3))
	})

	res, err := c.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusOK || res.RecordsSynced != 3 {
		t.Fatalf("got %+v", res)
	}
	if res.Message != "59 API calls remaining this window" {
		t.Errorf("unexpected message %q", res.Message)
	}
}

func TestGitHub_FollowsPagination(t *testing.T) {
	var srvURL string
	c := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", fmt.Sprintf(`<%s/next?page=2>; rel="next", <%s/next?page=3>; rel="last"`, srvURL, srvURL))
			fmt.Fprint(w, reposJSON(100))
		case "2":
			w.Header().Set("Link", fmt.Sprintf(`<%s/next?page=3>; rel="next"`, srvURL))
			fmt.Fprint(w, reposJSON(100))
		case "3":
			fmt.Fprint(w, reposJSON(42))
		}
	})
	srvURL = c.BaseURL

	res, _ := c.Sync(context.Background())
	if res.Status != StatusOK || res.RecordsSynced != 242 {
		t.Fatalf("expected 242 repos across 3 pages, got %+v", res)
	}
}

func TestGitHub_StopsRunawayPagination(t *testing.T) {
	var srvURL string
	c := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<%s/loop>; rel="next"`, srvURL)) // always another page
		fmt.Fprint(w, reposJSON(1))
	})
	srvURL = c.BaseURL

	res, _ := c.Sync(context.Background())
	if res.Status != StatusError {
		t.Fatalf("expected error status for endless pagination, got %+v", res)
	}
}

func TestGitHub_StatusMapping(t *testing.T) {
	tests := []struct {
		name      string
		code      int
		remaining string
		body      string
		want      Status
	}{
		{"429 is rate limited", http.StatusTooManyRequests, "", "", StatusRateLimited},
		{"403 with zero quota is rate limited", http.StatusForbidden, "0", "", StatusRateLimited},
		{"403 with quota left is an error", http.StatusForbidden, "10", "", StatusError},
		{"404 is an error", http.StatusNotFound, "", "", StatusError},
		{"malformed JSON is an error", http.StatusOK, "", "{not json", StatusError},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestGitHub(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.remaining != "" {
					w.Header().Set("X-RateLimit-Remaining", tc.remaining)
				}
				w.WriteHeader(tc.code)
				fmt.Fprint(w, tc.body)
			})
			res, err := c.Sync(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if res.Status != tc.want {
				t.Fatalf("got status %s (%s), want %s", res.Status, res.Message, tc.want)
			}
		})
	}
}

func TestGitHub_NetworkErrorIsReported(t *testing.T) {
	c := NewGitHubConnector("x", "")
	c.BaseURL = "http://127.0.0.1:1" // nothing listens here

	res, err := c.Sync(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusError {
		t.Fatalf("expected error status, got %+v", res)
	}
}

func TestNextPageURL(t *testing.T) {
	if got := nextPageURL(`<https://a/b?page=2>; rel="next", <https://a/b?page=5>; rel="last"`); got != "https://a/b?page=2" {
		t.Errorf("got %q", got)
	}
	if got := nextPageURL(`<https://a/b?page=5>; rel="last"`); got != "" {
		t.Errorf("expected no next page, got %q", got)
	}
	if got := nextPageURL(""); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}
