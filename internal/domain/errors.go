package domain

import "errors"

var (
	// ErrNotFound is returned when a resource is not found
	ErrNotFound = errors.New("not found")

	// ErrInvalidInput is returned when input validation fails
	ErrInvalidInput = errors.New("invalid input")

	// ErrConnectionFailed is returned when connection to external service fails
	ErrConnectionFailed = errors.New("connection failed")

	// ErrSubscriptionFailed is returned when subscription to events fails
	ErrSubscriptionFailed = errors.New("subscription failed")

	// ErrPublishFailed is returned when publishing to message queue fails
	ErrPublishFailed = errors.New("publish failed")
)
