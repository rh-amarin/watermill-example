package main

import (
	"context"
	"flag"
	"fmt"
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
		brokerType = flag.String("broker", "rabbitmq", "Broker type: rabbitmq or googlepubsub")
		topic      = flag.String("topic", "test-topic", "Topic name")
	)
	flag.Parse()

	logger := watermill.NewStdLogger(false, false)

	var ps interface{ Close() error }
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

	runPublisher(ctx, ps, *brokerType, *topic, logger)
}

func runPublisher(ctx context.Context, ps interface{ Close() error }, brokerType, topic string, logger watermill.LoggerAdapter) {
	logger.Info("Starting publisher", watermill.LogFields{"topic": topic})

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	messageCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-sigChan:
			logger.Info("Shutting down publisher", nil)
			return
		case <-ticker.C:
			messageCount++
			// Create NodePoolEvent
			nodePoolEvent := &events.NodePoolEvent{
				ClusterID:  int32(messageCount),
				ID:         int32(messageCount * 10),
				Href:       fmt.Sprintf("/api/clusters/%d/nodepools/%d", messageCount, messageCount*10),
				Generation: int32(1),
			}

			// Create EventMessage with NodePoolEvent as the payload type
			eventMsg := &pubsub.EventMessage[events.NodePoolEvent]{
				ID:       fmt.Sprintf("event-%d", messageCount),
				Type:     events.CloudEventType,
				Source:   "watermill-publisher",
				Payload:  *nodePoolEvent,
				Metadata: map[string]string{"count": fmt.Sprintf("%d", messageCount)},
			}

			// Publish EventMessage using type assertion
			var err error
			switch p := ps.(type) {
			case *pubsub.RabbitMQPubSub:
				err = pubsub.PublishRabbitMQ(ctx, p, topic, eventMsg)
			case *pubsub.GooglePubSubPubSub:
				err = pubsub.PublishGooglePubSub(ctx, p, topic, eventMsg)
			default:
				log.Fatalf("Unsupported pubsub type for publishing: %T", ps)
			}

			if err != nil {
				logger.Error("Failed to publish EventMessage", err, watermill.LogFields{
					"topic":      topic,
					"message_id": eventMsg.ID,
				})
				continue
			}

			logger.Info("Published EventMessage", watermill.LogFields{
				"topic":       topic,
				"message_id":  eventMsg.ID,
				"count":       messageCount,
				"cluster_id":  nodePoolEvent.ClusterID,
				"nodepool_id": nodePoolEvent.ID,
			})
		}
	}
}
