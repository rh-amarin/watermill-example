package pubsub

import (
	"context"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// MessageHandler is a function that processes messages
type MessageHandler func(ctx context.Context, msg *Message) error

// Message represents a message in the pub/sub system
type Message struct {
	UUID      string
	Payload   []byte
	Metadata  map[string]string
	CreatedAt time.Time
}

// FromWatermillMessage converts a watermill message to our Message type
func FromWatermillMessage(msg *message.Message) *Message {
	metadata := make(map[string]string)
	// Copy metadata from watermill message
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	return &Message{
		UUID:      msg.UUID,
		Payload:   msg.Payload,
		Metadata:  metadata,
		CreatedAt: time.Now(),
	}
}

// ToWatermillMessage converts our Message type to a watermill message
func (m *Message) ToWatermillMessage() *message.Message {
	msg := message.NewMessage(m.UUID, m.Payload)
	if m.Metadata != nil {
		for k, v := range m.Metadata {
			msg.Metadata.Set(k, v)
		}
	}
	return msg
}

// Publisher is an interface for publishing messages
type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Message) error
	Close() error
}

// Subscriber is an interface for subscribing to messages
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}

// PubSub combines Publisher and Subscriber interfaces
type PubSub interface {
	Publisher
	Subscriber
}

// BrokerType represents the type of message broker
type BrokerType string

const (
	BrokerTypeRabbitMQ   BrokerType = "rabbitmq"
	BrokerTypeGooglePubSub BrokerType = "googlepubsub"
)

// Config holds configuration for creating a PubSub instance
type Config struct {
	BrokerType BrokerType
	Logger     watermill.LoggerAdapter
	
	// RabbitMQ specific config
	RabbitMQURL string
	
	// Google Pub/Sub specific config
	GoogleProjectID string
	GoogleCredentialsPath string
}

// NewPubSub creates a new PubSub instance based on the broker type
func NewPubSub(config Config) (PubSub, error) {
	switch config.BrokerType {
	case BrokerTypeRabbitMQ:
		return NewRabbitMQPubSub(config)
	case BrokerTypeGooglePubSub:
		return NewGooglePubSubPubSub(config)
	default:
		return nil, ErrUnsupportedBrokerType
	}
}

