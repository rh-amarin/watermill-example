package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
)

// RabbitMQPubSub implements PubSub using RabbitMQ
type RabbitMQPubSub struct {
	publisher      *amqp.Publisher
	subscriber     *amqp.Subscriber
	logger         watermill.LoggerAdapter
	workerPoolSize int
}

// NewRabbitMQPubSub creates a new RabbitMQ PubSub instance
func NewRabbitMQPubSub(config Config) (PubSub, error) {
	if config.RabbitMQURL == "" {
		return nil, fmt.Errorf("rabbitmq url is required")
	}

	amqpConfig := amqp.NewDurablePubSubConfig(config.RabbitMQURL, amqp.GenerateQueueNameTopicNameWithSuffix(""))

	publisher, err := amqp.NewPublisher(amqpConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create rabbitmq publisher: %w", err)
	}

	subscriber, err := amqp.NewSubscriber(amqpConfig, config.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create rabbitmq subscriber: %w", err)
	}

	workerPoolSize := config.WorkerPoolSize
	if workerPoolSize <= 0 {
		workerPoolSize = 1
	}

	return &RabbitMQPubSub{
		publisher:      publisher,
		subscriber:     subscriber,
		logger:         config.Logger,
		workerPoolSize: workerPoolSize,
	}, nil
}

// Publish publishes an EventMessage to the specified topic
func (r *RabbitMQPubSub) Publish(ctx context.Context, topic string, event *EventMessage) error {
	if r.publisher == nil {
		return ErrPublisherNotInitialized
	}

	watermillMsg, err := eventMessageToWatermillMessage(event)
	if err != nil {
		return fmt.Errorf("failed to convert EventMessage to watermill message: %w", err)
	}

	return r.publisher.Publish(topic, watermillMsg)
}

// Subscribe subscribes to messages from the specified topic
func (r *RabbitMQPubSub) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	if r.subscriber == nil {
		return ErrSubscriberNotInitialized
	}

	messages, err := r.subscriber.Subscribe(ctx, topic)
	if err != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	// Create and start worker pool
	pool := newWorkerPool(r.workerPoolSize, r.logger, topic)
	pool.start(ctx)

	// Start message loop goroutine
	go func() {
		defer pool.stop()

		for {
			select {
			case <-ctx.Done():
				r.logger.Info("subscribe loop cancelled, shutting down", watermill.LogFields{
					"topic": topic,
				})
				return
			case msg, ok := <-messages:
				if !ok {
					// Channel closed
					r.logger.Info("message channel closed", watermill.LogFields{
						"topic": topic,
					})
					return
				}

				// Submit job to worker pool
				pool.submit(messageJob{
					msg:     msg,
					handler: handler,
					ctx:     ctx,
					logger:  r.logger,
					topic:   topic,
				})
			}
		}
	}()

	return nil
}

// Close closes the publisher and subscriber
func (r *RabbitMQPubSub) Close() error {
	var errs []error

	if r.publisher != nil {
		if err := r.publisher.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close publisher: %w", err))
		}
	}

	if r.subscriber != nil {
		if err := r.subscriber.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close subscriber: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing rabbitmq pubsub: %v", errs)
	}

	return nil
}
