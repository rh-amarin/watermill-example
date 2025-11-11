package pubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRabbitMQPubSub_Integration(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start RabbitMQ container
	rabbitmqContainer, err := rabbitmq.Run(ctx,
		"rabbitmq:3.12.11-management-alpine",
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Time to start").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := rabbitmqContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get connection string
	amqpURL, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(false, false)

	// Create PubSub instance
	config := RabbitMQConfig{
		Logger: logger,
		URL:    amqpURL,
	}
	ps, err := NewRabbitMQPubSub(config)
	require.NoError(t, err)
	defer ps.Close()

	topic := "test-topic"

	// Test subscribing - set up subscription BEFORE publishing
	received := make(chan *NodePoolEvent, 1)
	handler := func(ctx context.Context, msg *EventMessage[NodePoolEvent]) error {
		// No double marshalling! msg.Payload is already NodePoolEvent
		received <- &msg.Payload
		return nil
	}

	err = SubscribeRabbitMQ(ctx, ps, topic, handler)
	require.NoError(t, err)

	// Wait for subscription to be ready (queue binding)
	time.Sleep(500 * time.Millisecond)

	// Test publishing - now that subscription is set up
	nodePoolEvent := NodePoolEvent{
		ClusterID:  1,
		ID:         10,
		Href:       "/api/clusters/1/nodepools/10",
		Generation: 1,
	}
	eventMsg := &EventMessage[NodePoolEvent]{
		ID:       "test-event-1",
		Type:     CloudEventType,
		Source:   CloudEventSource,
		Payload:  nodePoolEvent,
		Metadata: map[string]string{"key": "value"},
	}

	err = PublishRabbitMQ(ctx, ps, topic, eventMsg)
	require.NoError(t, err)

	// Wait for message
	select {
	case receivedEvent := <-received:
		assert.NotNil(t, receivedEvent)
		assert.Equal(t, nodePoolEvent.ClusterID, receivedEvent.ClusterID)
		assert.Equal(t, nodePoolEvent.ID, receivedEvent.ID)
		assert.Equal(t, nodePoolEvent.Href, receivedEvent.Href)
		assert.Equal(t, nodePoolEvent.Generation, receivedEvent.Generation)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestRabbitMQPubSub_MultipleSubscribers(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start RabbitMQ container
	rabbitmqContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3"),
		rabbitmq.WithAdminUsername("guest"),
		rabbitmq.WithAdminPassword("guest"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").
				WithOccurrence(1).
				WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	defer func() {
		if err := rabbitmqContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	amqpURL, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(false, false)

	config := RabbitMQConfig{
		Logger: logger,
		URL:    amqpURL,
	}

	// Create publisher
	publisher, err := NewRabbitMQPubSub(config)
	require.NoError(t, err)
	defer publisher.Close()

	// Create two subscribers
	subscriber1, err := NewRabbitMQPubSub(config)
	require.NoError(t, err)
	defer subscriber1.Close()

	subscriber2, err := NewRabbitMQPubSub(config)
	require.NoError(t, err)
	defer subscriber2.Close()

	topic := "load-balance-topic"

	// Set up subscribers
	received1 := make(chan *NodePoolEvent, 10)
	received2 := make(chan *NodePoolEvent, 10)

	handler1 := func(ctx context.Context, msg *EventMessage[NodePoolEvent]) error {
		received1 <- &msg.Payload
		return nil
	}

	handler2 := func(ctx context.Context, msg *EventMessage[NodePoolEvent]) error {
		received2 <- &msg.Payload
		return nil
	}

	err = SubscribeRabbitMQ(ctx, subscriber1, topic, handler1)
	require.NoError(t, err)

	err = SubscribeRabbitMQ(ctx, subscriber2, topic, handler2)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(500 * time.Millisecond)

	// Publish multiple messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		nodePoolEvent := NodePoolEvent{
			ClusterID:  int32(i),
			ID:         int32(i * 10),
			Href:       fmt.Sprintf("/api/clusters/%d/nodepools/%d", i, i*10),
			Generation: int32(i),
		}
		eventMsg := &EventMessage[NodePoolEvent]{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    CloudEventType,
			Source:  CloudEventSource,
			Payload: nodePoolEvent,
		}
		err = PublishRabbitMQ(ctx, publisher, topic, eventMsg)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for messages to be distributed
	time.Sleep(2 * time.Second)

	// Verify messages were distributed between subscribers
	totalReceived := len(received1) + len(received2)
	assert.Equal(t, numMessages, totalReceived, "All messages should be received")
	assert.Greater(t, len(received1), 0, "Subscriber 1 should receive at least one message")
	assert.Greater(t, len(received2), 0, "Subscriber 2 should receive at least one message")
}

