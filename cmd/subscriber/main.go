package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/events"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
)

func main() {
	var (
		topic        = flag.String("topic", "test-topic", "Topic name")
		subscriberID = flag.String("subscriber-id", "", "Subscriber ID (required)")
	)
	flag.Parse()

	if *subscriberID == "" {
		log.Fatal("subscriber-id is required")
	}

	logger := watermill.NewStdLogger(false, false)

	// Use factory function to create PubSub from configuration
	ps, err := pubsub.NewPubSubFromConfig(logger)
	if err != nil {
		log.Fatalf("Failed to create pubsub: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runSubscriber(ctx, ps, *topic, *subscriberID, logger)
}

func runSubscriber(ctx context.Context, ps interface{ Close() error }, topic, subscriberID string, logger watermill.LoggerAdapter) {
	logger.Info("Starting subscriber", watermill.LogFields{
		"topic":         topic,
		"subscriber_id": subscriberID,
	})

	// Use Subscribe to avoid double marshalling - payload is already typed!
	handler := func(ctx context.Context, msg *pubsub.EventMessage[events.NodePoolEvent]) error {
		// No double marshalling! msg.Payload is already events.NodePoolEvent
		logger.Info("Received CloudEvent", watermill.LogFields{
			"subscriber_id": subscriberID,
			"message_id":    msg.ID,
			"ce_id":         msg.ID,
			"ce_type":       msg.Type,
			"ce_source":     msg.Source,
			"cluster_id":    msg.Payload.ClusterID, // Direct access, no marshalling needed!
			"nodepool_id":   msg.Payload.ID,
			"href":          msg.Payload.Href,
			"generation":    msg.Payload.Generation,
		})

		// Simulate processing time
		time.Sleep(100 * time.Millisecond)

		return nil
	}

	// Use Subscribe function that hides broker implementation details
	if err := pubsub.Subscribe(ctx, ps, topic, handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Shutting down subscriber", watermill.LogFields{"subscriber_id": subscriberID})
}
