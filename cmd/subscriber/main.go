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
	"github.com/asyncapi-cloudevents/watermill-abstraction/internal/config"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/events"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
)

func main() {
	var (
		brokerType   = flag.String("broker", "rabbitmq", "Broker type: rabbitmq or googlepubsub")
		topic        = flag.String("topic", "test-topic", "Topic name")
		subscriberID = flag.String("subscriber-id", "", "Subscriber ID (required)")
	)
	flag.Parse()

	if *subscriberID == "" {
		log.Fatal("subscriber-id is required")
	}

	logger := watermill.NewStdLogger(false, false)

	var ps pubsub.PubSub
	var err error
	switch *brokerType {
	case "rabbitmq":
		rmqq := config.LoadRabbitMQConfig(logger)
		ps, err = pubsub.NewRabbitMQPubSub(rmqq)
	case "googlepubsub":
		gcfg := config.LoadGooglePubSubConfig(logger)
		ps, err = pubsub.NewGooglePubSub(gcfg)
	default:
		log.Fatalf("Unsupported broker type: %s", *brokerType)
	}
	if err != nil {
		log.Fatalf("Failed to create pubsub: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runSubscriber(ctx, ps, *topic, *subscriberID, logger)
}

func runSubscriber(ctx context.Context, ps pubsub.PubSub, topic, subscriberID string, logger watermill.LoggerAdapter) {
	logger.Info("Starting subscriber", watermill.LogFields{
		"topic":         topic,
		"subscriber_id": subscriberID,
	})

	// Use SubscribeTyped to avoid double marshalling - payload is already typed!
	handler := func(ctx context.Context, msg *pubsub.TypedEventMessage[events.NodePoolEvent]) error {
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

	// Type assert to concrete type to access SubscribeTyped function
	// Since Go doesn't support generic methods on non-generic types, we use standalone functions
	switch p := ps.(type) {
	case *pubsub.RabbitMQPubSub:
		if err := pubsub.SubscribeTypedRabbitMQ(ctx, p, topic, handler); err != nil {
			log.Fatalf("Failed to subscribe: %v", err)
		}
	case *pubsub.GooglePubSubPubSub:
		if err := pubsub.SubscribeTypedGooglePubSub(ctx, p, topic, handler); err != nil {
			log.Fatalf("Failed to subscribe: %v", err)
		}
	default:
		log.Fatalf("Unsupported pubsub type for typed subscription: %T", ps)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Shutting down subscriber", watermill.LogFields{"subscriber_id": subscriberID})
}
