package config

import (
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
)

// LoadRabbitMQConfig reads env and returns a RabbitMQ-specific config
func LoadRabbitMQConfig(logger watermill.LoggerAdapter) pubsub.RabbitMQConfig {
	rabbitMQURL := os.Getenv("RABBITMQ_URL")
	if rabbitMQURL == "" {
		rabbitMQURL = "amqp://guest:guest@localhost:5672/"
	}
	return pubsub.RabbitMQConfig{
		Logger: logger,
		URL:    rabbitMQURL,
	}
}

// LoadGooglePubSubConfig reads env and returns a Google Pub/Sub-specific config
func LoadGooglePubSubConfig(logger watermill.LoggerAdapter) pubsub.GooglePubSubConfig {
	projectID := os.Getenv("GOOGLE_PROJECT_ID")
	if projectID == "" {
		projectID = "test-project"
	}
	credentialsPath := os.Getenv("GOOGLE_CREDENTIALS_PATH")
	return pubsub.GooglePubSubConfig{
		Logger:          logger,
		ProjectID:       projectID,
		CredentialsPath: credentialsPath,
	}
}
