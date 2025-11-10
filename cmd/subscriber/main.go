package main

import (
	"context"
	"encoding/json"
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

	cfg, err := config.LoadPubSubConfig(*brokerType, logger)
	if err != nil {
		log.Fatalf("Failed to load pubsub config: %v", err)
	}

	ps, err := pubsub.NewPubSub(cfg)
	if err != nil {
		log.Fatalf("Failed to create pubsub: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runSubscriber(ctx, ps, *topic, *subscriberID, logger)
}

func runSubscriber(ctx context.Context, ps pubsub.Subscriber, topic, subscriberID string, logger watermill.LoggerAdapter) {
	logger.Info("Starting subscriber", watermill.LogFields{
		"topic":         topic,
		"subscriber_id": subscriberID,
	})

	handler := func(ctx context.Context, msg *pubsub.EventMessage) error {
		// Extract NodePoolEvent from EventMessage payload
		var nodePoolEvent events.NodePoolEvent
		if msg.Payload != nil {
			// Marshal payload to JSON and unmarshal into NodePoolEvent
			dataBytes, err := json.Marshal(msg.Payload)
			if err != nil {
				logger.Error("Failed to marshal EventMessage payload", err, watermill.LogFields{
					"message_id": msg.ID,
				})
				return err
			}
			if err := json.Unmarshal(dataBytes, &nodePoolEvent); err != nil {
				logger.Error("Failed to unmarshal NodePoolEvent", err, watermill.LogFields{
					"message_id": msg.ID,
				})
				return err
			}
		}

		logger.Info("Received CloudEvent", watermill.LogFields{
			"subscriber_id": subscriberID,
			"message_id":    msg.ID,
			"ce_id":         msg.ID,
			"ce_type":       msg.Type,
			"ce_source":     msg.Source,
			"cluster_id":    nodePoolEvent.ClusterID,
			"nodepool_id":   nodePoolEvent.ID,
			"href":          nodePoolEvent.Href,
			"generation":    nodePoolEvent.Generation,
		})

		// Simulate processing time
		time.Sleep(100 * time.Millisecond)

		return nil
	}

	if err := ps.Subscribe(ctx, topic, handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Info("Shutting down subscriber", watermill.LogFields{"subscriber_id": subscriberID})
}
