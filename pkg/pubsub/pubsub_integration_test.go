package pubsub

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"
)

func xTestRabbitMQPubSub_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start RabbitMQ container
	rabbitmqContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3-management-alpine"),
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

	// Get connection string
	amqpURL, err := rabbitmqContainer.AmqpURL(ctx)
	require.NoError(t, err)

	logger := watermill.NewStdLogger(false, false)

	// Create PubSub instance
	config := Config{
		BrokerType:  BrokerTypeRabbitMQ,
		Logger:      logger,
		RabbitMQURL: amqpURL,
	}

	ps, err := NewRabbitMQPubSub(config)
	require.NoError(t, err)
	defer ps.Close()

	topic := "test-topic"

	// Test publishing
	eventMsg := &EventMessage[any]{
		ID:       "test-event-1",
		Type:     "test.type",
		Source:   "test-source",
		Payload:  map[string]string{"message": "hello"},
		Metadata: map[string]string{"key": "value"},
	}

	err = ps.Publish(ctx, topic, eventMsg)
	require.NoError(t, err)

	// Test subscribing
	received := make(chan *Message, 1)
	handler := func(ctx context.Context, msg *Message) error {
		received <- msg
		return nil
	}

	err = ps.Subscribe(ctx, topic, handler)
	require.NoError(t, err)

	// Wait for message
	select {
	case msg := <-received:
		assert.NotNil(t, msg)
		assert.Equal(t, eventMsg.ID, msg.UUID)
		assert.NotEmpty(t, msg.Payload)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func xTestRabbitMQPubSub_MultipleSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start RabbitMQ container
	rabbitmqContainer, err := rabbitmq.RunContainer(ctx,
		testcontainers.WithImage("rabbitmq:3-management-alpine"),
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

	config := Config{
		BrokerType:  BrokerTypeRabbitMQ,
		Logger:      logger,
		RabbitMQURL: amqpURL,
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
	received1 := make(chan *Message, 10)
	received2 := make(chan *Message, 10)

	handler1 := func(ctx context.Context, msg *Message) error {
		received1 <- msg
		return nil
	}

	handler2 := func(ctx context.Context, msg *Message) error {
		received2 <- msg
		return nil
	}

	err = subscriber1.Subscribe(ctx, topic, handler1)
	require.NoError(t, err)

	err = subscriber2.Subscribe(ctx, topic, handler2)
	require.NoError(t, err)

	// Give subscribers time to set up
	time.Sleep(500 * time.Millisecond)

	// Publish multiple messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		eventMsg := &EventMessage[any]{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    "test.type",
			Source:  "test-source",
			Payload: map[string]int{"count": i},
		}
		err = publisher.Publish(ctx, topic, eventMsg)
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

func TestGooglePubSubPubSub_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start Google Pub/Sub emulator container
	pubsubContainer, err := testcontainers.Run(ctx,
		"gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		testcontainers.WithExposedPorts("8085/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("Server started, listening on"),
				wait.ForListeningPort("8085/tcp"),
			).WithDeadline(60*time.Second),
		),
		testcontainers.WithCmd(
			"gcloud", "beta", "emulators", "pubsub", "start",
			"--host-port=0.0.0.0:8085",
			"--project=test-project",
			"--verbosity=debug",
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := pubsubContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get emulator host
	host, err := pubsubContainer.Host(ctx)
	require.NoError(t, err)

	port, err := pubsubContainer.MappedPort(ctx, "8085")
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())

	// Set environment variable for Pub/Sub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	logger := watermill.NewStdLogger(false, false)

	// Create PubSub instance
	config := Config{
		BrokerType:      BrokerTypeGooglePubSub,
		Logger:          logger,
		GoogleProjectID: "test-project",
	}

	ps, err := NewGooglePubSubPubSub(config)
	require.NoError(t, err)
	defer ps.Close()

	topic := "test-topic"

	// IMPORTANT: In Google Pub/Sub, subscription must exist BEFORE publishing messages
	// Set up subscription first
	received := make(chan *Message, 1)
	handler := func(ctx context.Context, msg *Message) error {
		received <- msg
		return nil
	}

	err = ps.Subscribe(ctx, topic, handler)
	require.NoError(t, err)

	// Wait for subscription to be fully set up
	// Google Pub/Sub needs time to create the topic and subscription
	time.Sleep(2 * time.Second)

	// Now publish the message
	eventMsg := &EventMessage[any]{
		ID:       "test-event-1",
		Type:     "test.type",
		Source:   "test-source",
		Payload:  map[string]string{"message": "hello"},
		Metadata: map[string]string{"key": "value"},
	}

	err = ps.Publish(ctx, topic, eventMsg)
	require.NoError(t, err)

	// Give message time to propagate
	time.Sleep(500 * time.Second)

	// Wait for message
	select {
	case msg := <-received:
		assert.NotNil(t, msg)
		assert.Equal(t, eventMsg.ID, msg.UUID)
		assert.NotEmpty(t, msg.Payload)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func xTestGooglePubSubPubSub_MultipleSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Start Google Pub/Sub emulator container
	pubsubContainer, err := testcontainers.Run(ctx,
		"gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
		testcontainers.WithExposedPorts("8085/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("Server started, listening on"),
				wait.ForListeningPort("8085/tcp"),
			).WithDeadline(60*time.Second),
		),
		testcontainers.WithCmd(
			"gcloud", "beta", "emulators", "pubsub", "start",
			"--host-port=0.0.0.0:8085",
			"--project=test-project",
			"--verbosity=debug",
		),
	)
	require.NoError(t, err)
	defer func() {
		if err := pubsubContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Get emulator host
	host, err := pubsubContainer.Host(ctx)
	require.NoError(t, err)

	port, err := pubsubContainer.MappedPort(ctx, "8085")
	require.NoError(t, err)

	emulatorHost := fmt.Sprintf("%s:%s", host, port.Port())

	// Set environment variable for Pub/Sub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)
	defer os.Unsetenv("PUBSUB_EMULATOR_HOST")

	logger := watermill.NewStdLogger(false, false)

	config := Config{
		BrokerType:      BrokerTypeGooglePubSub,
		Logger:          logger,
		GoogleProjectID: "test-project",
	}

	// Create publisher
	publisher, err := NewGooglePubSubPubSub(config)
	require.NoError(t, err)
	defer publisher.Close()

	// Create two subscribers
	subscriber1, err := NewGooglePubSubPubSub(config)
	require.NoError(t, err)
	defer subscriber1.Close()

	subscriber2, err := NewGooglePubSubPubSub(config)
	require.NoError(t, err)
	defer subscriber2.Close()

	topic := "load-balance-topic"

	// Set up subscribers
	received1 := make(chan *Message, 10)
	received2 := make(chan *Message, 10)

	handler1 := func(ctx context.Context, msg *Message) error {
		received1 <- msg
		return nil
	}

	handler2 := func(ctx context.Context, msg *Message) error {
		received2 <- msg
		return nil
	}

	err = subscriber1.Subscribe(ctx, topic, handler1)
	require.NoError(t, err)

	err = subscriber2.Subscribe(ctx, topic, handler2)
	require.NoError(t, err)

	// Give subscribers time to set up (Google Pub/Sub needs time to create topics/subscriptions)
	//time.Sleep(2 * time.Second)

	// Publish multiple messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		eventMsg := &EventMessage[any]{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    "test.type",
			Source:  "test-source",
			Payload: map[string]int{"count": i},
		}
		err = publisher.Publish(ctx, topic, eventMsg)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)
	}

	// Wait for messages to be distributed
	//time.Sleep(3 * time.Second)

	// Verify messages were distributed between subscribers
	totalReceived := len(received1) + len(received2)
	assert.Equal(t, numMessages, totalReceived, "All messages should be received")
	assert.Greater(t, len(received1), 0, "Subscriber 1 should receive at least one message")
	assert.Greater(t, len(received2), 0, "Subscriber 2 should receive at least one message")
}
