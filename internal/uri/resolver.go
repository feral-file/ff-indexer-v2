package uri

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	logger "github.com/bitmark-inc/autonomy-logger"
	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

// Config holds configuration for the URI resolver
type Config struct {
	// IPFSGateways is the list of IPFS gateways to try
	IPFSGateways []string
	// ArweaveGateways is the list of Arweave gateways to try
	ArweaveGateways []string
}

type Resolver interface {
	// Resolve resolves the URI to a canonical URL
	// It handles special URL schemes like ipfs:// and ar://
	// Returns the canonical URL
	// It will make a HEAD request to the URL to check if it is accessible
	// If the URL is not accessible, it will return an error
	Resolve(ctx context.Context, uri string) (string, error)
}

type resolver struct {
	httpClient adapter.HTTPClient
	config     *Config
}

func NewResolver(httpClient adapter.HTTPClient, config *Config) Resolver {
	return &resolver{
		httpClient: httpClient,
		config:     config,
	}
}

func (r *resolver) Resolve(ctx context.Context, uri string) (string, error) {
	// Handle IPFS URLs
	if cid, ok := strings.CutPrefix(uri, "ipfs://"); ok {
		return r.resolveIPFS(ctx, cid)
	}

	// Handle Arweave URLs
	if txID, ok := strings.CutPrefix(uri, "ar://"); ok {
		return r.resolveArweave(ctx, txID)
	}

	// Handle IPFS gateway URLs (e.g., https://example.com/ipfs/QmXxx)
	if strings.Contains(uri, "/ipfs/") {
		// Extract CID from URL
		parts := strings.Split(uri, "/ipfs/")
		if len(parts) >= 2 {
			return r.resolveIPFS(ctx, parts[1])
		}
	}

	// Regular HTTP(S) URL
	return uri, nil
}

// resolveIPFS finds a working IPFS gateway for the given CID
func (r *resolver) resolveIPFS(ctx context.Context, cid string) (string, error) {
	if len(r.config.IPFSGateways) == 0 {
		return "", fmt.Errorf("no IPFS gateways configured")
	}

	logger.Info("Resolving IPFS CID", zap.String("cid", cid), zap.Int("gateways", len(r.config.IPFSGateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(r.config.IPFSGateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range r.config.IPFSGateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/ipfs/%s", gw, cid)
			resp, err := r.httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.Warn("failed to close response body", zap.Error(err), zap.String("url", url))
			}

			if resp.StatusCode == http.StatusOK {
				resultCh <- result{url: url}
			} else {
				resultCh <- result{err: fmt.Errorf("gateway returned status %d", resp.StatusCode)}
			}
		}(gateway)
	}

	// Wait for all goroutines in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Return the first successful result
	for res := range resultCh {
		if res.err == nil {
			logger.Info("Found working IPFS gateway", zap.String("url", res.url))
			return res.url, nil
		}
	}

	return "", fmt.Errorf("no working IPFS gateway found for CID: %s", cid)
}

// resolveArweave finds a working Arweave gateway for the given transaction ID
func (r *resolver) resolveArweave(ctx context.Context, txID string) (string, error) {
	if len(r.config.ArweaveGateways) == 0 {
		return "", fmt.Errorf("no Arweave gateways configured")
	}

	logger.Info("Resolving Arweave TX", zap.String("txID", txID), zap.Int("gateways", len(r.config.ArweaveGateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(r.config.ArweaveGateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range r.config.ArweaveGateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := gw + txID
			resp, err := r.httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.Warn("failed to close response body", zap.Error(err), zap.String("url", url))
			}

			if resp.StatusCode == http.StatusOK {
				resultCh <- result{url: url}
			} else {
				resultCh <- result{err: fmt.Errorf("gateway returned status %d", resp.StatusCode)}
			}
		}(gateway)
	}

	// Wait for all goroutines in a separate goroutine
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Return the first successful result
	for res := range resultCh {
		if res.err == nil {
			logger.Info("Found working Arweave gateway", zap.String("url", res.url))
			return res.url, nil
		}
	}

	return "", fmt.Errorf("no working Arweave gateway found for TX: %s", txID)
}
