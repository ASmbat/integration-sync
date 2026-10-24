package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

const (
	defaultGitHubBaseURL = "https://api.github.com"
	// maxPages stops a runaway pagination loop (e.g. a bad Link header).
	maxPages = 50
)

// GitHubConnector syncs public repository metadata for a GitHub user.
// It handles auth headers, Link-header pagination, rate-limit headers, and
// maps HTTP failures onto a normalized SyncResult.
type GitHubConnector struct {
	Username string
	Token    string // optional; unauthenticated requests have a low rate limit
	BaseURL  string // defaults to the public API; tests point it at httptest
	client   *http.Client
}

func NewGitHubConnector(username, token string) *GitHubConnector {
	return &GitHubConnector{
		Username: username,
		Token:    token,
		BaseURL:  defaultGitHubBaseURL,
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *GitHubConnector) ID() string          { return "github" }
func (c *GitHubConnector) DisplayName() string { return "GitHub" }

type githubRepo struct {
	Name      string    `json:"name"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Sync does a full sync of the user's public repositories, following
// pagination until the last page. Vendor problems (network errors, rate
// limits, bad responses) are reported in the SyncResult, not as an error,
// so the scheduler records them as status instead of failing.
func (c *GitHubConnector) Sync(ctx context.Context) (SyncResult, error) {
	next := fmt.Sprintf("%s/users/%s/repos?per_page=100", c.BaseURL, url.PathEscape(c.Username))
	total := 0
	var last *http.Response

	for page := 0; next != ""; page++ {
		if page == maxPages {
			return failed(StatusError, fmt.Sprintf("stopped after %d pages", maxPages)), nil
		}

		repos, resp, result, ok := c.fetchPage(ctx, next)
		if !ok {
			return result, nil
		}
		total += len(repos)
		last = resp
		next = nextPageURL(resp.Header.Get("Link"))
	}

	return SyncResult{
		Status:        StatusOK,
		RecordsSynced: total,
		Message:       rateLimitSummary(last),
		SyncedAt:      time.Now(),
	}, nil
}

// fetchPage requests one page. When ok is false, result describes the failure.
func (c *GitHubConnector) fetchPage(ctx context.Context, pageURL string) (repos []githubRepo, resp *http.Response, result SyncResult, ok bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return nil, nil, failed(StatusError, "building request: "+err.Error()), false
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err = c.client.Do(req)
	if err != nil {
		return nil, nil, failed(StatusError, err.Error()), false
	}
	body := resp.Body // not resp: it is a named result and may be nil when this runs
	defer func() {
		// Drain a little of any unread body so the connection can be reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
		body.Close()
	}()

	// GitHub signals rate limiting with 429, or 403 plus a zero remaining
	// quota. Telling this apart from a real rejection lets callers back off
	// instead of treating it as a hard failure.
	if resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode == http.StatusForbidden && resp.Header.Get("X-RateLimit-Remaining") == "0") {
		return nil, nil, failed(StatusRateLimited, "GitHub API rate limit hit, will retry next cycle"), false
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, failed(StatusError, fmt.Sprintf("unexpected status %d", resp.StatusCode)), false
	}

	if err := json.NewDecoder(resp.Body).Decode(&repos); err != nil {
		return nil, nil, failed(StatusError, "malformed response: "+err.Error()), false
	}
	return repos, resp, SyncResult{}, true
}

var linkNextRe = regexp.MustCompile(`<([^>]+)>\s*;\s*rel="next"`)

// nextPageURL extracts the rel="next" URL from a GitHub Link header, or
// returns "" when there is no next page.
func nextPageURL(link string) string {
	if m := linkNextRe.FindStringSubmatch(link); m != nil {
		return m[1]
	}
	return ""
}

func failed(status Status, msg string) SyncResult {
	return SyncResult{Status: status, Message: msg, SyncedAt: time.Now()}
}

func rateLimitSummary(resp *http.Response) string {
	if resp == nil {
		return ""
	}
	if n, err := strconv.Atoi(resp.Header.Get("X-RateLimit-Remaining")); err == nil {
		return fmt.Sprintf("%d API calls remaining this window", n)
	}
	return ""
}
