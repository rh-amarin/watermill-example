package config

import (
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
)

// LoadPubSubConfig creates a pubsub.Config based on the broker type and environment variables
func LoadPubSubConfig(brokerType string, logger watermill.LoggerAdapter) (pubsub.Config, error) {
	switch brokerType {
	case "rabbitmq":
		rabbitMQURL := os.Getenv("RABBITMQ_URL")
		if rabbitMQURL == "" {
			rabbitMQURL = "amqp://guest:guest@localhost:5672/"
		}
		return pubsub.Config{
			BrokerType:  pubsub.BrokerTypeRabbitMQ,
			Logger:      logger,
			RabbitMQURL: rabbitMQURL,
		}, nil

	case "googlepubsub":
		projectID := os.Getenv("GOOGLE_PROJECT_ID")
		if projectID == "" {
			projectID = "test-project"
		}
		return pubsub.Config{
			BrokerType:      pubsub.BrokerTypeGooglePubSub,
			Logger:          logger,
			GoogleProjectID: projectID,
		}, nil

	default:
		return pubsub.Config{}, &UnsupportedBrokerTypeError{BrokerType: brokerType}
	}
}

// UnsupportedBrokerTypeError represents an error when an unsupported broker type is provided
type UnsupportedBrokerTypeError struct {
	BrokerType string
}

func (e *UnsupportedBrokerTypeError) Error() string {
	return "unsupported broker type: " + e.BrokerType
}

