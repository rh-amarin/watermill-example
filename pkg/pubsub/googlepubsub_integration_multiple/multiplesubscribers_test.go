package googlepubsub_integration_multiple

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

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

	config := pubsub.GooglePubSubConfig{
		Logger:    logger,
		ProjectID: "test-project",
	}

	// Create publisher
	publisher, err := pubsub.NewGooglePubSub(config)
	require.NoError(t, err)
	defer publisher.Close()

	// Create two subscribers
	subscriber1, err := pubsub.NewGooglePubSub(config)
	require.NoError(t, err)
	defer subscriber1.Close()

	subscriber2, err := pubsub.NewGooglePubSub(config)
	require.NoError(t, err)
	defer subscriber2.Close()

	topic := "load-balance-topic"

	// Set up subscribers
	received1 := make(chan *pubsub.NodePoolEvent, 10)
	received2 := make(chan *pubsub.NodePoolEvent, 10)

	handler1 := func(ctx context.Context, msg *pubsub.EventMessage[pubsub.NodePoolEvent]) error {
		t.Log("handler1")
		t.Logf("Received message: %+v", msg)
		received1 <- &msg.Payload
		return nil
	}

	handler2 := func(ctx context.Context, msg *pubsub.EventMessage[pubsub.NodePoolEvent]) error {
		t.Log("handler2")
		t.Logf("Received message: %+v", msg)
		received2 <- &msg.Payload
		return nil
	}

	err = pubsub.SubscribeGooglePubSub(ctx, subscriber1, topic, handler1)
	require.NoError(t, err)

	err = pubsub.SubscribeGooglePubSub(ctx, subscriber2, topic, handler2)
	require.NoError(t, err)

	// Give subscribers time to set up (Google Pub/Sub needs time to create topics/subscriptions)
	// Wait longer to ensure both subscribers are fully connected and ready
	time.Sleep(3 * time.Second)

	// Publish multiple messages
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		nodePoolEvent := pubsub.NodePoolEvent{
			ClusterID:  int32(i),
			ID:         int32(i * 10),
			Href:       fmt.Sprintf("/api/clusters/%d/nodepools/%d", i, i*10),
			Generation: int32(i),
		}
		eventMsg := &pubsub.EventMessage[pubsub.NodePoolEvent]{
			ID:      fmt.Sprintf("test-event-%d", i),
			Type:    pubsub.CloudEventType,
			Source:  pubsub.CloudEventSource,
			Payload: nodePoolEvent,
		}
		err = pubsub.PublishGooglePubSub(ctx, publisher, topic, eventMsg)
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

