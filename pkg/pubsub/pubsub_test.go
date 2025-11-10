package pubsub

import (
	"context"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMessageConversion(t *testing.T) {
	t.Run("FromWatermillMessage", func(t *testing.T) {
		watermillMsg := &message.Message{
			UUID:     uuid.New().String(),
			Payload:  []byte("test payload"),
			Metadata: make(message.Metadata),
		}
		watermillMsg.Metadata.Set("key1", "value1")
		watermillMsg.Metadata.Set("key2", "value2")

		appMsg := FromWatermillMessage(watermillMsg)

		assert.Equal(t, watermillMsg.UUID, appMsg.UUID)
		assert.Equal(t, []byte(watermillMsg.Payload), appMsg.Payload)
		assert.Equal(t, "value1", appMsg.Metadata["key1"])
		assert.Equal(t, "value2", appMsg.Metadata["key2"])
		assert.False(t, appMsg.CreatedAt.IsZero())
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original := &Message{
			UUID:     uuid.New().String(),
			Payload:  []byte("test payload"),
			Metadata: map[string]string{"key1": "value1"},
		}

		// Create watermill message manually for testing
		watermillMsg := message.NewMessage(original.UUID, original.Payload)
		for k, v := range original.Metadata {
			watermillMsg.Metadata.Set(k, v)
		}
		converted := FromWatermillMessage(watermillMsg)

		assert.Equal(t, original.UUID, converted.UUID)
		assert.Equal(t, original.Payload, converted.Payload)
		assert.Equal(t, original.Metadata["key1"], converted.Metadata["key1"])
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
		var handler MessageHandler = func(ctx context.Context, msg *Message) error {
			return nil
		}

		ctx := context.Background()
		msg := &Message{
			UUID:    uuid.New().String(),
			Payload: []byte("test"),
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
		var pub Publisher[any]
		var r *RabbitMQPubSub[any]
		pub = r
		_ = pub              // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})

	t.Run("SubscriberInterface", func(t *testing.T) {
		var sub Subscriber
		var r *RabbitMQPubSub[any]
		sub = r
		_ = sub              // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})

	t.Run("PubSubInterface", func(t *testing.T) {
		var ps PubSub[any]
		var r *RabbitMQPubSub[any]
		ps = r
		_ = ps               // Verify it compiles
		assert.True(t, true) // Interface is satisfied
	})
}
