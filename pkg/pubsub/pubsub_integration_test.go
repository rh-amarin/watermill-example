package pubsub

import (
	"context"
	"encoding/json"
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

// NodePoolEvent represents a node pool event for testing
type NodePoolEvent struct {
	ClusterID  int32  `json:"clusterId"`
	ID         int32  `json:"id"`
	Href       string `json:"href"`
	Generation int32  `json:"generation"`
}

const (
	cloudEventType   = "com.example.nodepool.event"
	cloudEventSource = "watermill-publisher"
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
	handler := func(ctx context.Context, msg *EventMessage) error {
		// Extract NodePoolEvent from payload
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received <- &nodePoolEvent
		return nil
	}

	err = ps.Subscribe(ctx, topic, handler)
	require.NoError(t, err)

	// Wait for subscription to be ready (queue binding)
	time.Sleep(500 * time.Millisecond)

	// Test publishing - now that subscription is set up
	nodePoolEvent := &NodePoolEvent{
		ClusterID:  1,
		ID:         10,
		Href:       "/api/clusters/1/nodepools/10",
		Generation: 1,
	}
	eventMsg := &EventMessage{
		ID:       "test-event-1",
		Type:     cloudEventType,
		Source:   cloudEventSource,
		Payload:  nodePoolEvent,
		Metadata: map[string]string{"key": "value"},
	}

	err = ps.Publish(ctx, topic, eventMsg)
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

	handler1 := func(ctx context.Context, msg *EventMessage) error {
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received1 <- &nodePoolEvent
		return nil
	}

	handler2 := func(ctx context.Context, msg *EventMessage) error {
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received2 <- &nodePoolEvent
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
		nodePoolEvent := &NodePoolEvent{
			ClusterID:  int32(i),
			ID:         int32(i * 10),
			Href:       fmt.Sprintf("/api/clusters/%d/nodepools/%d", i, i*10),
			Generation: int32(i),
		}
		eventMsg := &EventMessage{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    cloudEventType,
			Source:  cloudEventSource,
			Payload: nodePoolEvent,
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
	t.Parallel()
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
	config := GooglePubSubConfig{
		Logger:    logger,
		ProjectID: "test-project",
	}

	ps, err := NewGooglePubSub(config)
	require.NoError(t, err)
	defer ps.Close()

	topic := "test-topic"

	// IMPORTANT: In Google Pub/Sub, subscription must exist BEFORE publishing messages
	// Set up subscription first
	received := make(chan *NodePoolEvent, 1)
	handler := func(ctx context.Context, msg *EventMessage) error {
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received <- &nodePoolEvent
		return nil
	}

	err = ps.Subscribe(ctx, topic, handler)
	require.NoError(t, err)

	// Wait for subscription to be fully set up
	// Google Pub/Sub needs time to create the topic and subscription
	time.Sleep(2 * time.Second)

	// Now publish the message
	nodePoolEvent := &NodePoolEvent{
		ClusterID:  1,
		ID:         10,
		Href:       "/api/clusters/1/nodepools/10",
		Generation: 1,
	}
	eventMsg := &EventMessage{
		ID:       "test-event-1",
		Type:     cloudEventType,
		Source:   cloudEventSource,
		Payload:  nodePoolEvent,
		Metadata: map[string]string{"key": "value"},
	}

	err = ps.Publish(ctx, topic, eventMsg)
	require.NoError(t, err)

	// Give message time to propagate
	time.Sleep(500 * time.Millisecond)

	// Wait for message
	select {
	case receivedEvent := <-received:
		assert.NotNil(t, receivedEvent)
		assert.Equal(t, nodePoolEvent.ClusterID, receivedEvent.ClusterID)
		assert.Equal(t, nodePoolEvent.ID, receivedEvent.ID)
		assert.Equal(t, nodePoolEvent.Href, receivedEvent.Href)
		assert.Equal(t, nodePoolEvent.Generation, receivedEvent.Generation)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for message")
	}
}

func TestGooglePubSubPubSub_MultipleSubscribers(t *testing.T) {
	t.Parallel()
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

	config := GooglePubSubConfig{
		Logger:    logger,
		ProjectID: "test-project",
	}

	// Create publisher
	publisher, err := NewGooglePubSub(config)
	require.NoError(t, err)
	defer publisher.Close()

	// Create two subscribers
	subscriber1, err := NewGooglePubSub(config)
	require.NoError(t, err)
	defer subscriber1.Close()

	subscriber2, err := NewGooglePubSub(config)
	require.NoError(t, err)
	defer subscriber2.Close()

	topic := "load-balance-topic"

	// Set up subscribers
	received1 := make(chan *NodePoolEvent, 10)
	received2 := make(chan *NodePoolEvent, 10)

	handler1 := func(ctx context.Context, msg *EventMessage) error {
		t.Log("handler1")
		t.Logf("Received message: %+v", msg)
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received1 <- &nodePoolEvent
		return nil
	}

	handler2 := func(ctx context.Context, msg *EventMessage) error {
		t.Log("handler2")
		t.Logf("Received message: %+v", msg)
		var nodePoolEvent NodePoolEvent
		if msg.Payload != nil {
			dataBytes, err := json.Marshal(msg.Payload)
			require.NoError(t, err)
			err = json.Unmarshal(dataBytes, &nodePoolEvent)
			require.NoError(t, err)
		}
		received2 <- &nodePoolEvent
		return nil
	}

	err = subscriber1.Subscribe(ctx, topic, handler1)
	require.NoError(t, err)

	err = subscriber2.Subscribe(ctx, topic, handler2)
	require.NoError(t, err)

	// Give subscribers time to set up (Google Pub/Sub needs time to create topics/subscriptions)
	// Wait longer to ensure both subscribers are fully connected and ready
	time.Sleep(3 * time.Second)

	// Publish multiple messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		nodePoolEvent := NodePoolEvent{
			ClusterID:  int32(i),
			ID:         int32(i * 10),
			Href:       fmt.Sprintf("/api/clusters/%d/nodepools/%d", i, i*10),
			Generation: int32(i),
		}
		eventMsg := &EventMessage{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    cloudEventType,
			Source:  cloudEventSource,
			Payload: nodePoolEvent,
		}
		err = publisher.Publish(ctx, topic, eventMsg)
		require.NoError(t, err)
		time.Sleep(200 * time.Millisecond)
	}

	// Wait longer for messages to be distributed between subscribers
	totalReceived := 0
	for i := 0; i < 25; i++ { // up to 25 seconds
		totalReceived = len(received1) + len(received2)
		t.Logf("Total received: %d in %d seconds", totalReceived, i)
		if totalReceived >= numMessages {
			break
		}
		time.Sleep(1 * time.Second)
	}
	t.Log("force test 2")

	// Verify messages were distributed between subscribers
	totalReceived = len(received1) + len(received2)
	assert.Equal(t, numMessages, totalReceived, "All messages should be received")
	assert.Greater(t, len(received1), 0, "Subscriber 1 should receive at least one message")
	assert.Greater(t, len(received2), 0, "Subscriber 2 should receive at least one message")
}
