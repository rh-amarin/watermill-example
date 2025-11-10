package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
	"github.com/google/uuid"
)

type MessagePayload struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
}

func main() {
	var (
		brokerType = flag.String("broker", "rabbitmq", "Broker type: rabbitmq or googlepubsub")
		topic      = flag.String("topic", "test-topic", "Topic name")
		mode       = flag.String("mode", "publisher", "Mode: publisher or subscriber")
		subscriberID = flag.String("subscriber-id", "", "Subscriber ID (required for subscriber mode)")
	)
	flag.Parse()

	logger := watermill.NewStdLogger(false, false)

	var config pubsub.Config
	switch *brokerType {
	case "rabbitmq":
		rabbitMQURL := os.Getenv("RABBITMQ_URL")
		if rabbitMQURL == "" {
			rabbitMQURL = "amqp://guest:guest@localhost:5672/"
		}
		config = pubsub.Config{
			BrokerType: pubsub.BrokerTypeRabbitMQ,
			Logger:     logger,
			RabbitMQURL: rabbitMQURL,
		}
	case "googlepubsub":
		projectID := os.Getenv("GOOGLE_PROJECT_ID")
		if projectID == "" {
			projectID = "test-project"
		}
		config = pubsub.Config{
			BrokerType: pubsub.BrokerTypeGooglePubSub,
			Logger:     logger,
			GoogleProjectID: projectID,
		}
	default:
		log.Fatalf("Unsupported broker type: %s", *brokerType)
	}

	ps, err := pubsub.NewPubSub(config)
	if err != nil {
		log.Fatalf("Failed to create pubsub: %v", err)
	}
	defer ps.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch *mode {
	case "publisher":
		runPublisher(ctx, ps, *topic, logger)
	case "subscriber":
		if *subscriberID == "" {
			log.Fatal("subscriber-id is required for subscriber mode")
		}
		runSubscriber(ctx, ps, *topic, *subscriberID, logger)
	default:
		log.Fatalf("Invalid mode: %s", *mode)
	}
}

func runPublisher(ctx context.Context, ps pubsub.Publisher, topic string, logger watermill.LoggerAdapter) {
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
			payload := MessagePayload{
				ID:        uuid.New().String(),
				Timestamp: time.Now(),
				Data:      fmt.Sprintf("Message #%d", messageCount),
			}

			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				logger.Error("Failed to marshal payload", err, nil)
				continue
			}

			msg := &pubsub.Message{
				UUID:     uuid.New().String(),
				Payload:  payloadBytes,
				Metadata: map[string]string{"source": "publisher"},
			}

			if err := ps.Publish(ctx, topic, msg); err != nil {
				logger.Error("Failed to publish message", err, watermill.LogFields{
					"topic": topic,
					"message_id": msg.UUID,
				})
				continue
			}

			logger.Info("Published message", watermill.LogFields{
				"topic": topic,
				"message_id": msg.UUID,
				"count": messageCount,
			})
		}
	}
}

func runSubscriber(ctx context.Context, ps pubsub.Subscriber, topic, subscriberID string, logger watermill.LoggerAdapter) {
	logger.Info("Starting subscriber", watermill.LogFields{
		"topic": topic,
		"subscriber_id": subscriberID,
	})

	handler := func(ctx context.Context, msg *pubsub.Message) error {
		var payload MessagePayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			logger.Error("Failed to unmarshal payload", err, watermill.LogFields{
				"message_id": msg.UUID,
			})
			return err
		}

		logger.Info("Received message", watermill.LogFields{
			"subscriber_id": subscriberID,
			"message_id": msg.UUID,
			"payload_id": payload.ID,
			"data": payload.Data,
			"timestamp": payload.Timestamp.Format(time.RFC3339),
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

