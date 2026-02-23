package uri

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"go.uber.org/zap"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/logger"
)

// FindWorkingIPFSGateway finds a working IPFS gateway for the given CID
// It tries all gateways in parallel and returns the first working one
func FindWorkingIPFSGateway(ctx context.Context, httpClient adapter.HTTPClient, cid string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no IPFS gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working IPFS gateway", zap.String("cid", cid), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/ipfs/%s", gw, cid)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
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
			logger.InfoCtx(ctx, "Found working IPFS gateway", zap.String("url", res.url))
			return res.url, nil
		}
	}

	return "", fmt.Errorf("no working IPFS gateway found for CID: %s", cid)
}

// FindWorkingArweaveGateway finds a working Arweave gateway for the given transaction ID
// It tries all gateways in parallel and returns the first working one
func FindWorkingArweaveGateway(ctx context.Context, httpClient adapter.HTTPClient, txID string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no Arweave gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working Arweave gateway", zap.String("txID", txID), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/%s", gw, txID)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
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
			logger.InfoCtx(ctx, "Found working Arweave gateway", zap.String("url", res.url))
			return res.url, nil
		}
	}

	return "", fmt.Errorf("no working Arweave gateway found for TX: %s", txID)
}

// FindWorkingOnChFSGateway finds a working OnChFS gateway for the given hash
// It tries all gateways in parallel and returns the first working one
// The hash parameter is a Keccak-256 hash (64 hex characters)
func FindWorkingOnChFSGateway(ctx context.Context, httpClient adapter.HTTPClient, hash string, gateways []string) (string, error) {
	if len(gateways) == 0 {
		return "", fmt.Errorf("no OnChFS gateways configured")
	}

	logger.InfoCtx(ctx, "Finding working OnChFS gateway", zap.String("hash", hash), zap.Int("gateways", len(gateways)))

	// Try all gateways in parallel
	type result struct {
		url string
		err error
	}

	resultCh := make(chan result, len(gateways))
	var wg sync.WaitGroup

	// Test each gateway with HEAD request
	for _, gateway := range gateways {
		wg.Add(1)
		go func(gw string) {
			defer wg.Done()

			url := fmt.Sprintf("%s/%s", gw, hash)
			resp, err := httpClient.Head(ctx, url)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			if err := resp.Body.Close(); err != nil {
				logger.WarnCtx(ctx, "failed to close response body", zap.Error(err), zap.String("url", url))
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
			logger.InfoCtx(ctx, "Found working OnChFS gateway", zap.String("url", res.url))
			return res.url, nil
		}
	}

	return "", fmt.Errorf("no working OnChFS gateway found for hash: %s", hash)
}
