package domain

import "errors"

var (
	// ErrSubscriptionFailed is returned when subscription to events fails
	ErrSubscriptionFailed = errors.New("subscription failed")

	// ErrTokenAlreadyExists is returned when attempting to mint a token that already exists
	ErrTokenAlreadyExists = errors.New("token already exists")

	// ErrTokenNotFound is returned when a token is not found
	ErrTokenNotFound = errors.New("token not found")

	// ErrTokenNotFoundOnChain is returned when a token does not exist on the blockchain
	ErrTokenNotFoundOnChain = errors.New("token not found on chain")

	// ErrInvalidTokenCID is returned when a token CID is invalid
	ErrInvalidTokenCID = errors.New("invalid token CID")

	// ErrInvalidBlockchainEvent is returned when a blockchain event is invalid
	ErrInvalidBlockchainEvent = errors.New("invalid blockchain event")

	// ErrUnsupportedMediaFile is returned when a media file is not supported
	ErrUnsupportedMediaFile = errors.New("unsupported media file")
)
