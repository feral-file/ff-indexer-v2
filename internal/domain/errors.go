package domain

import "errors"

var (
	// ErrSubscriptionFailed is returned when subscription to events fails
	ErrSubscriptionFailed = errors.New("subscription failed")

	// ErrTokenAlreadyExists is returned when attempting to mint a token that already exists
	ErrTokenAlreadyExists = errors.New("token already exists")

	// ErrTokenNotFound is returned when a token is not found
	ErrTokenNotFound = errors.New("token not found")
)
