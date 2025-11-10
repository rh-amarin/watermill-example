# Watermill Pub/Sub Abstraction

A simple abstraction layer over Watermill library (v1.5.1) for publishing and subscribing to messages, making it easy to switch between different message brokers (RabbitMQ, Google Pub/Sub) with minimal code changes.

## Features

- **Simple Abstraction**: Clean interface for publish/subscribe operations
- **Multiple Brokers**: Support for RabbitMQ and Google Pub/Sub
- **Easy Broker Switching**: Change brokers by updating configuration only
- **Load Balancing**: Multiple subscribers can share the same topic subscription
- **Docker Examples**: Ready-to-use examples with Docker Compose

## Project Structure

```
.
├── pkg/pubsub/          # Abstraction layer
│   ├── pubsub.go       # Core interfaces and types
│   ├── rabbitmq.go     # RabbitMQ implementation
│   ├── googlepubsub.go # Google Pub/Sub implementation
│   └── pubsub_test.go  # Unit tests
├── cmd/app/            # Example application (publisher/subscriber)
│   └── main.go
├── examples/
│   ├── rabbitmq/       # RabbitMQ example with docker-compose
│   └── googlepubsub/    # Google Pub/Sub example with docker-compose
└── Dockerfile          # Dockerfile for building the application

```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
    "github.com/ThreeDotsLabs/watermill"
)

func main() {
    logger := watermill.NewStdLogger(false, false)
    
    // Create PubSub instance (RabbitMQ example)
    config := pubsub.Config{
        BrokerType: pubsub.BrokerTypeRabbitMQ,
        Logger:     logger,
        RabbitMQURL: "amqp://guest:guest@localhost:5672/",
    }
    
    ps, err := pubsub.NewPubSub(config)
    if err != nil {
        panic(err)
    }
    defer ps.Close()
    
    ctx := context.Background()
    
    // Publish a message
    msg := &pubsub.Message{
        UUID:    "123",
        Payload: []byte("Hello, World!"),
        Metadata: map[string]string{"source": "example"},
    }
    ps.Publish(ctx, "test-topic", msg)
    
    // Subscribe to messages
    ps.Subscribe(ctx, "test-topic", func(ctx context.Context, msg *pubsub.Message) error {
        println("Received:", string(msg.Payload))
        return nil
    })
}
```

### Switching Brokers

To switch from RabbitMQ to Google Pub/Sub, simply change the configuration:

```go
config := pubsub.Config{
    BrokerType: pubsub.BrokerTypeGooglePubSub,
    Logger:     logger,
    GoogleProjectID: "my-project",
}
```

## Running Examples

### RabbitMQ Example

1. Start the RabbitMQ broker and applications:
```bash
cd examples/rabbitmq
docker-compose up
```

This will start:
- RabbitMQ broker (port 5672, management UI on 15672)
- One publisher (publishes messages every 2 seconds)
- Two subscribers (sharing the same topic subscription, load balancing messages)

### Google Pub/Sub Example

1. Start the Google Pub/Sub emulator and applications:
```bash
cd examples/googlepubsub
docker-compose up
```

This will start:
- Google Pub/Sub emulator (port 8085)
- One publisher (publishes messages every 2 seconds)
- Two subscribers (sharing the same topic subscription, load balancing messages)

## Testing

Run all tests:
```bash
make test
```

Or manually:
```bash
go test ./pkg/pubsub/... -v
```

## Building Docker Images

Build the Docker image:
```bash
make build
```

Or manually:
```bash
docker build -t watermill-app .
```

## Requirements

- Go 1.23 or later
- Docker and Docker Compose (for examples)
- RabbitMQ (for RabbitMQ example) or Google Cloud SDK emulator (for Pub/Sub example)

## License

MIT

