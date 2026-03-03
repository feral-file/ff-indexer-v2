package adapter

import (
	"context"
	"time"

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
	Evaluate(expr string, result interface{}, options ...chromedp.EvaluateOption) chromedp.EvaluateAction
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

func (c *RealChromedpClient) Evaluate(expr string, result interface{}, options ...chromedp.EvaluateOption) chromedp.EvaluateAction {
	return chromedp.Evaluate(expr, result, options...)
}
