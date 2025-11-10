package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-googlecloud/v2/pkg/googlecloud"
	"google.golang.org/api/option"
)

// GooglePubSubPubSub implements PubSub using Google Cloud Pub/Sub
type GooglePubSubPubSub struct {
	publisher  *googlecloud.Publisher
	subscriber *googlecloud.Subscriber
	logger     watermill.LoggerAdapter
}

// NewGooglePubSubPubSub creates a new Google Pub/Sub instance
func NewGooglePubSubPubSub(config Config) (PubSub, error) {
	if config.GoogleProjectID == "" {
		return nil, fmt.Errorf("google project id is required")
	}

	var opts []option.ClientOption
	if config.GoogleCredentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(config.GoogleCredentialsPath))
	}
	// If no credentials path is provided, use default credentials (works with emulator via PUBSUB_EMULATOR_HOST)

	publisherConfig := googlecloud.PublisherConfig{
		ProjectID:     config.GoogleProjectID,
		ClientOptions: opts,
	}

	publisher, err := googlecloud.NewPublisher(publisherConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create google pubsub publisher: %w", err)
	}

	subscriberConfig := googlecloud.SubscriberConfig{
		ProjectID:     config.GoogleProjectID,
		ClientOptions: opts,
		GenerateSubscriptionName: func(topic string) string {
			return topic + "-subscription"
		},
	}

	subscriber, err := googlecloud.NewSubscriber(subscriberConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create google pubsub subscriber: %w", err)
	}

	return &GooglePubSubPubSub{
		publisher:  publisher,
		subscriber: subscriber,
		logger:     config.Logger,
	}, nil
}

// Publish publishes an EventMessage to the specified topic
func (g *GooglePubSubPubSub) Publish(ctx context.Context, topic string, event *EventMessage) error {
	if g.publisher == nil {
		return ErrPublisherNotInitialized
	}

	watermillMsg, err := eventMessageToWatermillMessage(event)
	if err != nil {
		return fmt.Errorf("failed to convert EventMessage to watermill message: %w", err)
	}

	return g.publisher.Publish(topic, watermillMsg)
}

// Subscribe subscribes to messages from the specified topic
func (g *GooglePubSubPubSub) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	if g.subscriber == nil {
		return ErrSubscriberNotInitialized
	}

	messages, err := g.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	go func() {
		for msg := range messages {
			eventMsg, err := watermillMessageToEventMessage(msg)
			if err != nil {
				g.logger.Error("failed to convert message to EventMessage", err, watermill.LogFields{
					"topic":      topic,
					"message_id": msg.UUID,
				})
				msg.Nack()
				continue
			}
			if err := handler(ctx, eventMsg); err != nil {
				g.logger.Error("failed to handle message", err, watermill.LogFields{
					"topic":      topic,
					"message_id": msg.UUID,
				})
				msg.Nack()
				continue
			}
			msg.Ack()
		}
	}()

	return nil
}

// Close closes the publisher and subscriber
func (g *GooglePubSubPubSub) Close() error {
	var errs []error

	if g.publisher != nil {
		if err := g.publisher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close publisher: %w", err))
		}
	}

	if g.subscriber != nil {
		if err := g.subscriber.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close subscriber: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing google pubsub: %v", errs)
	}

	return nil
}
