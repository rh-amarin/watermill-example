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

		eventMsg, err := watermillMessageToEventMessage(watermillMsg)
		require.NoError(t, err)

		assert.Equal(t, "test-id-123", eventMsg.ID)
		assert.Equal(t, "test.type", eventMsg.Type)
		assert.Equal(t, "test.source", eventMsg.Source)
		assert.NotNil(t, eventMsg.Payload)
		assert.Equal(t, "value1", eventMsg.Metadata["key1"])
		assert.Equal(t, "value2", eventMsg.Metadata["key2"])
	})

	t.Run("InvalidJSON", func(t *testing.T) {
		watermillMsg := &message.Message{
			UUID:     uuid.New().String(),
			Payload:  []byte("invalid json"),
			Metadata: make(message.Metadata),
		}

		_, err := watermillMessageToEventMessage(watermillMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse CloudEvent")
	})
}

func TestNewPubSub(t *testing.T) {
	t.Run("UnsupportedBrokerType", func(t *testing.T) {
		config := Config{
			BrokerType: BrokerType("unsupported"),
			Logger:     watermill.NopLogger{},
		}

		_, err := NewPubSub(config)
		assert.Error(t, err)
		assert.Equal(t, ErrUnsupportedBrokerType, err)
	})

	t.Run("RabbitMQMissingURL", func(t *testing.T) {
		config := Config{
			BrokerType: BrokerTypeRabbitMQ,
			Logger:     watermill.NopLogger{},
		}

		_, err := NewPubSub(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rabbitmq url is required")
	})

	t.Run("GooglePubSubMissingProjectID", func(t *testing.T) {
		config := Config{
			BrokerType: BrokerTypeGooglePubSub,
			Logger:     watermill.NopLogger{},
		}

		_, err := NewPubSub(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "google project id is required")
	})
}

func TestMessageHandler(t *testing.T) {
	t.Run("HandlerSignature", func(t *testing.T) {
		var handler MessageHandler = func(ctx context.Context, msg *EventMessage) error {
			return nil
		}

		ctx := context.Background()
		msg := &EventMessage{
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
	t.Run("BrokerTypeConstants", func(t *testing.T) {
		assert.Equal(t, BrokerType("rabbitmq"), BrokerTypeRabbitMQ)
		assert.Equal(t, BrokerType("googlepubsub"), BrokerTypeGooglePubSub)
	})

	t.Run("ConfigWithLogger", func(t *testing.T) {
		logger := watermill.NopLogger{}
		config := Config{
			BrokerType: BrokerTypeRabbitMQ,
			Logger:     logger,
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

// Integration test helpers (these would require actual broker connections)
func TestPubSubInterface(t *testing.T) {
	t.Run("PublisherInterface", func(t *testing.T) {
		var pub Publisher
		var r *RabbitMQPubSub
		pub = r
		_ = pub              // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})

	t.Run("SubscriberInterface", func(t *testing.T) {
		var sub Subscriber
		var r *RabbitMQPubSub
		sub = r
		_ = sub              // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})

	t.Run("PubSubInterface", func(t *testing.T) {
		var ps PubSub
		var r *RabbitMQPubSub
		ps = r
		_ = ps               // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})
}
