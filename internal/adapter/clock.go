package adapter

import "time"

// Clock defines an interface for time operations to enable mocking
//
//go:generate mockgen -source=clock.go -destination=../mocks/clock.go -package=mocks -mock_names=Clock=MockClock
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
	Parse(layout, value string) (time.Time, error)
	Unix(sec int64, nsec int64) time.Time
	After(d time.Duration) <-chan time.Time
}

// RealClock implements Clock using the standard time package
type RealClock struct{}

// NewClock creates a new real clock implementation
func NewClock() Clock {
	return &RealClock{}
}

func (c *RealClock) Now() time.Time {
	return time.Now()
}

func (c *RealClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (c *RealClock) Sleep(d time.Duration) {
	time.Sleep(d)
}

func (c *RealClock) Parse(layout, value string) (time.Time, error) {
	return time.Parse(layout, value)
}

func (c *RealClock) Unix(sec int64, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

func (c *RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
