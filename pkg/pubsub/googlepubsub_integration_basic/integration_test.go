package googlepubsub_integration_basic

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
	config := pubsub.GooglePubSubConfig{
		Logger:    logger,
		ProjectID: "test-project",
	}

	ps, err := pubsub.NewGooglePubSub(config)
	require.NoError(t, err)
	defer ps.Close()

	topic := "test-topic"

	// IMPORTANT: In Google Pub/Sub, subscription must exist BEFORE publishing messages
	// Set up subscription first
	received := make(chan *pubsub.NodePoolEvent, 1)
	handler := func(ctx context.Context, msg *pubsub.EventMessage[pubsub.NodePoolEvent]) error {
		received <- &msg.Payload
		return nil
	}

	err = pubsub.SubscribeGooglePubSub(ctx, ps, topic, handler)
	require.NoError(t, err)

	// Wait for subscription to be fully set up
	// Google Pub/Sub needs time to create the topic and subscription
	time.Sleep(2 * time.Second)

	// Now publish the message
	nodePoolEvent := pubsub.NodePoolEvent{
		ClusterID:  1,
		ID:         10,
		Href:       "/api/clusters/1/nodepools/10",
		Generation: 1,
	}
	eventMsg := &pubsub.EventMessage[pubsub.NodePoolEvent]{
		ID:       "test-event-1",
		Type:     pubsub.CloudEventType,
		Source:   pubsub.CloudEventSource,
		Payload:  nodePoolEvent,
		Metadata: map[string]string{"key": "value"},
	}

	err = pubsub.PublishGooglePubSub(ctx, ps, topic, eventMsg)
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

