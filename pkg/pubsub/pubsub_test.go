package pubsub

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatermillMessageToEventMessage(t *testing.T) {
	type TestPayload struct {
		Message string `json:"message"`
	}

	t.Run("ValidCloudEvent", func(t *testing.T) {
		// Create a CloudEvent JSON payload
		cloudEventJSON := `{
			"specversion": "1.0",
			"type": "test.type",
			"source": "test.source",
			"id": "test-id-123",
			"data": {"message": "hello"}
		}`

		watermillMsg := &message.Message{
			UUID:     "test-id-123",
			Payload:  []byte(cloudEventJSON),
			Metadata: make(message.Metadata),
		}
		watermillMsg.Metadata.Set("key1", "value1")
		watermillMsg.Metadata.Set("key2", "value2")

		eventMsg, err := watermillMessageToEventMessage[TestPayload](watermillMsg)
		require.NoError(t, err)

		assert.Equal(t, "test-id-123", eventMsg.ID)
		assert.Equal(t, "test.type", eventMsg.Type)
		assert.Equal(t, "test.source", eventMsg.Source)
		assert.Equal(t, "hello", eventMsg.Payload.Message)
		assert.Equal(t, "value1", eventMsg.Metadata["key1"])
		assert.Equal(t, "value2", eventMsg.Metadata["key2"])
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		watermillMsg := &message.Message{
			UUID:     uuid.New().String(),
			Payload:  []byte("invalid json"),
			Metadata: make(message.Metadata),
		}

		_, err := watermillMessageToEventMessage[TestPayload](watermillMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse CloudEvent")
	})
}

func TestNewPubSub(t *testing.T) {
	t.Run("RabbitMQMissingURL", func(t *testing.T) {
		config := RabbitMQConfig{
			Logger: watermill.NopLogger{},
			URL:    "",
		}

		_, err := NewRabbitMQPubSub(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rabbitmq url is required")
	})

	t.Run("GooglePubSubMissingProjectID", func(t *testing.T) {
		config := GooglePubSubConfig{
			Logger:    watermill.NopLogger{},
			ProjectID: "",
		}

		_, err := NewGooglePubSub(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "google project id is required")
	})
}

func TestMessageHandler(t *testing.T) {
	t.Run("HandlerSignature", func(t *testing.T) {
		var handler MessageHandler[string] = func(ctx context.Context, msg *EventMessage[string]) error {
			return nil
		}

		ctx := context.Background()
		msg := &EventMessage[string]{
			ID:      uuid.New().String(),
			Type:    "test.type",
			Source:  "test.source",
			Payload: "test",
		}

		err := handler(ctx, msg)
		assert.NoError(t, err)
	})
}

func TestConfig(t *testing.T) {
	t.Run("RabbitMQConfigWithLogger", func(t *testing.T) {
		logger := watermill.NopLogger{}
		config := RabbitMQConfig{
			Logger: logger,
			URL:    "amqp://guest:guest@localhost:5672/",
		}
		assert.Equal(t, logger, config.Logger)
	})

	t.Run("GooglePubSubConfigWithLogger", func(t *testing.T) {
		logger := watermill.NopLogger{}
		config := GooglePubSubConfig{
			Logger:    logger,
			ProjectID: "project",
		}
		assert.Equal(t, logger, config.Logger)
	})
}

func TestErrors(t *testing.T) {
	t.Run("ErrorMessages", func(t *testing.T) {
		assert.Error(t, ErrUnsupportedBrokerType)
		assert.Error(t, ErrPublisherNotInitialized)
		assert.Error(t, ErrSubscriberNotInitialized)
	})
}
