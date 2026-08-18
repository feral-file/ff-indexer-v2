package adapter

import (
	"context"
	"time"

	"github.com/chromedp/cdproto/cdp"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

// ChromedpClient defines an interface for chromedp operations to enable mocking
//
//go:generate mockgen -source=chromedp.go -destination=../mocks/chromedp.go -package=mocks -mock_names=ChromedpClient=MockChromedpClient
type ChromedpClient interface {
	NewExecAllocator(ctx context.Context, opts []chromedp.ExecAllocatorOption) (context.Context, context.CancelFunc)
	NewContext(ctx context.Context) (context.Context, context.CancelFunc)
	Run(ctx context.Context, actions ...chromedp.Action) error
	Navigate(url string) chromedp.NavigateAction
	WaitReady(sel string, waitReadyOpts ...chromedp.QueryOption) chromedp.QueryAction
	Sleep(duration time.Duration) chromedp.Action
	EmulateViewport(width, height int64) chromedp.EmulateAction
	FullScreenshot(result *[]byte, quality int) chromedp.Action
	// CaptureScreenshot captures only the current viewport, unlike FullScreenshot which
	// captures the whole scrollable document (unbounded for attacker-controlled pages).
	CaptureScreenshot(result *[]byte) chromedp.Action
	Evaluate(expr string, result interface{}, options ...chromedp.EvaluateOption) chromedp.EvaluateAction

	// --- request interception (SSRF enforcement for browser-initiated traffic) ---

	// AddScriptToEvaluateOnNewDocument returns an action installing source so it runs in
	// every new document before any page script — used to neutralize egress APIs the CDP
	// Fetch domain cannot intercept (WebSocket, WebRTC, EventSource).
	AddScriptToEvaluateOnNewDocument(source string) chromedp.Action
	// FetchEnable returns an action enabling the Fetch domain so that every request the
	// browser is about to make (navigations, redirects, subresources) is paused for a
	// policy decision.
	FetchEnable() chromedp.Action
	// NetworkEnable returns an action enabling the Network domain so response events
	// (EventRequestWillBeSent, EventResponseReceived) reach ListenTarget handlers — used
	// by the render probe to observe the main document's HTTP status.
	NetworkEnable() chromedp.Action
	// ListenTarget registers fn for target events on ctx; fn receives paused requests.
	ListenTarget(ctx context.Context, fn func(ev any))
	// ContinueRequest allows a paused request to proceed.
	ContinueRequest(ctx context.Context, id fetch.RequestID) error
	// FailRequest aborts a paused request with the given reason.
	FailRequest(ctx context.Context, id fetch.RequestID, reason network.ErrorReason) error
}

type RealChromedpClient struct{}

func NewChromedpClient() ChromedpClient {
	return &RealChromedpClient{}
}

func (c *RealChromedpClient) NewExecAllocator(ctx context.Context, opts []chromedp.ExecAllocatorOption) (context.Context, context.CancelFunc) {
	return chromedp.NewExecAllocator(ctx, opts...)
}

func (c *RealChromedpClient) NewContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return chromedp.NewContext(ctx)
}

func (c *RealChromedpClient) Run(ctx context.Context, actions ...chromedp.Action) error {
	return chromedp.Run(ctx, actions...)
}

func (c *RealChromedpClient) Navigate(url string) chromedp.NavigateAction {
	return chromedp.Navigate(url)
}

func (c *RealChromedpClient) WaitReady(sel string, waitReadyOpts ...chromedp.QueryOption) chromedp.QueryAction {
	return chromedp.WaitReady(sel, waitReadyOpts...)
}

func (c *RealChromedpClient) Sleep(duration time.Duration) chromedp.Action {
	return chromedp.Sleep(duration)
}

func (c *RealChromedpClient) EmulateViewport(width, height int64) chromedp.EmulateAction {
	return chromedp.EmulateViewport(width, height)
}

func (c *RealChromedpClient) FullScreenshot(result *[]byte, quality int) chromedp.Action {
	return chromedp.FullScreenshot(result, quality)
}

func (c *RealChromedpClient) CaptureScreenshot(result *[]byte) chromedp.Action {
	return chromedp.CaptureScreenshot(result)
}

func (c *RealChromedpClient) Evaluate(expr string, result interface{}, options ...chromedp.EvaluateOption) chromedp.EvaluateAction {
	return chromedp.Evaluate(expr, result, options...)
}

func (c *RealChromedpClient) AddScriptToEvaluateOnNewDocument(source string) chromedp.Action {
	return chromedp.ActionFunc(func(ctx context.Context) error {
		_, err := page.AddScriptToEvaluateOnNewDocument(source).Do(ctx)
		return err
	})
}

func (c *RealChromedpClient) FetchEnable() chromedp.Action {
	return fetch.Enable()
}

func (c *RealChromedpClient) NetworkEnable() chromedp.Action {
	return network.Enable()
}

func (c *RealChromedpClient) ListenTarget(ctx context.Context, fn func(ev any)) {
	chromedp.ListenTarget(ctx, func(ev interface{}) { fn(ev) })
}

// ContinueRequest and FailRequest run on the browser context's target executor, which is
// how CDP commands must be issued from inside an event handler.
func (c *RealChromedpClient) ContinueRequest(ctx context.Context, id fetch.RequestID) error {
	return fetch.ContinueRequest(id).Do(cdp.WithExecutor(ctx, chromedp.FromContext(ctx).Target))
}

func (c *RealChromedpClient) FailRequest(ctx context.Context, id fetch.RequestID, reason network.ErrorReason) error {
	return fetch.FailRequest(id, reason).Do(cdp.WithExecutor(ctx, chromedp.FromContext(ctx).Target))
}
