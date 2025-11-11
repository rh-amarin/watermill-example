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
	publisher      *googlecloud.Publisher
	subscriber     *googlecloud.Subscriber
	logger         watermill.LoggerAdapter
	workerPoolSize int
}

// NewGooglePubSub creates a new Google Pub/Sub instance
func NewGooglePubSub(config GooglePubSubConfig) (PubSub, error) {
	if config.ProjectID == "" {
		return nil, fmt.Errorf("google project id is required")
	}

	var opts []option.ClientOption
	if config.CredentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(config.CredentialsPath))
	}
	// If no credentials path is provided, use default credentials (works with emulator via PUBSUB_EMULATOR_HOST)

	publisherConfig := googlecloud.PublisherConfig{
		ProjectID:     config.ProjectID,
		ClientOptions: opts,
	}

	publisher, err := googlecloud.NewPublisher(publisherConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create google pubsub publisher: %w", err)
	}

	subscriberConfig := googlecloud.SubscriberConfig{
		ProjectID:     config.ProjectID,
		ClientOptions: opts,
	}
	// Allow caller to customize subscription naming; default to topic-based
	if config.GenerateSubscriptionName != nil {
		subscriberConfig.GenerateSubscriptionName = config.GenerateSubscriptionName
	} else {
		subscriberConfig.GenerateSubscriptionName = func(topic string) string {
			return topic + "-subscription"
		}
	}

	subscriber, err := googlecloud.NewSubscriber(subscriberConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create google pubsub subscriber: %w", err)
	}

	workerPoolSize := config.WorkerPoolSize
	if workerPoolSize <= 0 {
		workerPoolSize = 1
	}

	return &GooglePubSubPubSub{
		publisher:      publisher,
		subscriber:     subscriber,
		logger:         config.Logger,
		workerPoolSize: workerPoolSize,
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

	// Create and start worker pool
	pool := newWorkerPool(g.workerPoolSize, g.logger, topic)
	pool.start(ctx)

	// Start message loop goroutine
	go func() {
		defer pool.stop()

		for {
			select {
			case <-ctx.Done():
				g.logger.Info("subscribe loop cancelled, shutting down", watermill.LogFields{
					"topic": topic,
				})
				return
			case msg, ok := <-messages:
				if !ok {
					// Channel closed
					g.logger.Info("message channel closed", watermill.LogFields{
						"topic": topic,
					})
					return
				}

				// Submit job to worker pool
				pool.submit(messageJob{
					msg:     msg,
					handler: handler,
					ctx:     ctx,
					logger:  g.logger,
					topic:   topic,
				})
			}
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
