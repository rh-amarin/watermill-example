package pubsub

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
)

// RabbitMQPubSub implements PubSub using RabbitMQ
type RabbitMQPubSub struct {
	publisher  *amqp.Publisher
	subscriber *amqp.Subscriber
	logger     watermill.LoggerAdapter
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

	return &RabbitMQPubSub{
		publisher:  publisher,
		subscriber: subscriber,
		logger:     config.Logger,
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

	go func() {
		for msg := range messages {
			eventMsg, err := watermillMessageToEventMessage(msg)
			if err != nil {
				r.logger.Error("failed to convert message to EventMessage", err, watermill.LogFields{
					"topic":      topic,
					"message_id": msg.UUID,
				})
				msg.Nack()
				continue
			}
			if err := handler(ctx, eventMsg); err != nil {
				r.logger.Error("failed to handle message", err, watermill.LogFields{
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
