package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// GitHubConnector syncs public repository metadata for a GitHub user.
// It's a real, working example of the messiness the role description
// calls out: auth headers, rate-limit headers, pagination-shaped APIs,
// and mapping HTTP-layer failures onto a normalized SyncResult.
type GitHubConnector struct {
	Username string
	Token    string // optional; unauthenticated requests are heavily rate-limited
	client   *http.Client
}

func NewGitHubConnector(username, token string) *GitHubConnector {
	return &GitHubConnector{
		Username: username,
		Token:    token,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *GitHubConnector) ID() string          { return "github" }
func (c *GitHubConnector) DisplayName() string { return "GitHub" }

type githubRepo struct {
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (c *GitHubConnector) Sync(ctx context.Context) (SyncResult, error) {
	url := fmt.Sprintf("https://api.github.com/users/%s/repos?per_page=100", c.Username)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return SyncResult{}, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return SyncResult{
			Status:   StatusError,
			Message:  err.Error(),
			SyncedAt: time.Now(),
		}, nil // network errors are reported, not fatal to the caller
	}
	defer resp.Body.Close()

	// GitHub signals rate limiting via status 403/429 plus a remaining-quota
	// header; a real integration framework needs to distinguish "vendor
	// says slow down" from "vendor rejected the request" so the scheduler
	// can back off intelligently instead of just failing.
	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
		remaining := resp.Header.Get("X-RateLimit-Remaining")
		if remaining == "0" || resp.StatusCode == http.StatusTooManyRequests {
			return SyncResult{
				Status:   StatusRateLimited,
				Message:  "GitHub API rate limit hit, will retry next cycle",
				SyncedAt: time.Now(),
			}, nil
		}
	}

	if resp.StatusCode != http.StatusOK {
		return SyncResult{
			Status:   StatusError,
			Message:  fmt.Sprintf("unexpected status %d", resp.StatusCode),
			SyncedAt: time.Now(),
		}, nil
	}

	var repos []githubRepo
	if err := json.NewDecoder(resp.Body).Decode(&repos); err != nil {
		return SyncResult{
			Status:   StatusError,
			Message:  "malformed response: " + err.Error(),
			SyncedAt: time.Now(),
		}, nil
	}

	return SyncResult{
		Status:        StatusOK,
		RecordsSynced: len(repos),
		Message:       rateLimitSummary(resp),
		SyncedAt:      time.Now(),
	}, nil
}

func rateLimitSummary(resp *http.Response) string {
	remaining := resp.Header.Get("X-RateLimit-Remaining")
	if remaining == "" {
		return ""
	}
	if n, err := strconv.Atoi(remaining); err == nil {
		return fmt.Sprintf("%d API calls remaining this window", n)
	}
	return ""
}
