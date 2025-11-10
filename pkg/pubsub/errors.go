package pubsub

import "errors"

var (
	ErrUnsupportedBrokerType = errors.New("unsupported broker type")
	ErrPublisherNotInitialized = errors.New("publisher not initialized")
	ErrSubscriberNotInitialized = errors.New("subscriber not initialized")
)

