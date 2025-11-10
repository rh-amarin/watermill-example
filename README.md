# Watermill Pub/Sub Abstraction with CloudEvents

A simple abstraction layer over Watermill library (v1.5.1) for publishing and subscribing to messages using CloudEvents format, making it easy to switch between different message brokers (RabbitMQ, Google Pub/Sub) with minimal code changes.

## Features

- **CloudEvents Format**: All messages are transmitted using the CloudEvents specification
- **Simple Abstraction**: Clean interface for publish/subscribe operations
- **Multiple Brokers**: Support for RabbitMQ and Google Pub/Sub
- **Easy Broker Switching**: Change brokers by updating configuration only
- **Load Balancing**: Multiple subscribers can share the same topic subscription
- **Docker Examples**: Ready-to-use examples with Docker Compose
- **Type-Safe Events**: NodePoolEvent type with cluster and pool information

## Project Structure

```
.
├── pkg/pubsub/          # Abstraction layer
│   ├── pubsub.go       # Core interfaces and types
│   ├── rabbitmq.go     # RabbitMQ implementation
│   ├── googlepubsub.go # Google Pub/Sub implementation
│   └── pubsub_test.go  # Unit tests
├── pkg/events/          # CloudEvents utilities
│   └── events.go       # NodePoolEvent and CloudEvent conversion
├── internal/config/     # Shared configuration utilities
│   └── config.go       # PubSub configuration loader
├── cmd/
│   ├── publisher/      # Publisher command
│   │   └── main.go
│   └── subscriber/     # Subscriber command
│       └── main.go
├── examples/
│   ├── rabbitmq/       # RabbitMQ example with podman compose
│   └── googlepubsub/    # Google Pub/Sub example with podman compose
├── Dockerfile.publisher # Dockerfile for publisher
└── Dockerfile.subscriber # Dockerfile for subscriber

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
    
    // Publish an EventMessage
    eventMsg := &pubsub.EventMessage{
        ID:      "123",
        Type:    "example.type",
        Source:  "example-source",
        Payload: "Hello, World!",
        Metadata: map[string]string{"source": "example"},
    }
    ps.Publish(ctx, "test-topic", eventMsg)
    
    // Subscribe to messages
    ps.Subscribe(ctx, "test-topic", func(ctx context.Context, msg *pubsub.EventMessage) error {
        fmt.Printf("Received: ID=%s, Type=%s, Payload=%v\n", msg.ID, msg.Type, msg.Payload)
        return nil
    })
}
```

### Using CloudEvents with NodePoolEvent

The application uses CloudEvents format for all messages. The Publisher interface accepts CloudEvents directly, handling the conversion to Watermill messages internally.

Here's an example of publishing a NodePoolEvent:

```go
package main

import (
    "context"
    "github.com/asyncapi-cloudevents/watermill-abstraction/pkg/events"
    "github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
    "github.com/ThreeDotsLabs/watermill"
)

func main() {
    logger := watermill.NewStdLogger(false, false)
    
    // Create PubSub instance
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
    
    // Create a NodePoolEvent
    nodePoolEvent := &events.NodePoolEvent{
        ClusterID:  1,
        ID:         10,
        Href:       "/api/clusters/1/nodepools/10",
        Generation: 1,
    }
    
    // Create EventMessage with NodePoolEvent as payload
    eventMsg := &pubsub.EventMessage{
        ID:      "event-123",
        Type:    events.CloudEventType,
        Source:  events.CloudEventSource,
        Payload: nodePoolEvent,
    }
    
    // Publish EventMessage (conversion to CloudEvent happens internally)
    ps.Publish(ctx, "nodepool-events", eventMsg)
}
```

### Receiving CloudEvents

To receive and process CloudEvents:

```go
import (
    "encoding/json"
    "fmt"
)

handler := func(ctx context.Context, msg *pubsub.EventMessage) error {
    // Access CloudEvent fields directly from EventMessage
    fmt.Printf("Event ID: %s, Type: %s, Source: %s\n", 
        msg.ID, msg.Type, msg.Source)
    
    // Extract NodePoolEvent from EventMessage payload
    var nodePoolEvent events.NodePoolEvent
    if msg.Payload != nil {
        dataBytes, _ := json.Marshal(msg.Payload)
        json.Unmarshal(dataBytes, &nodePoolEvent)
    }
    
    // Process the event
    fmt.Printf("Received NodePoolEvent: ClusterID=%d, ID=%d\n", 
        nodePoolEvent.ClusterID, nodePoolEvent.ID)
    
    return nil
}

ps.Subscribe(ctx, "nodepool-events", handler)
```

### NodePoolEvent Structure

The `NodePoolEvent` type contains:
- `ClusterID` (int32): The cluster identifier
- `ID` (int32): The node pool identifier
- `Href` (string): The resource reference URL
- `Generation` (int32): The generation number

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
podman compose up
```

This will start:
- RabbitMQ broker (port 5672, management UI on 15672)
- One publisher (publishes messages every 2 seconds)
- Two subscribers (sharing the same topic subscription, load balancing messages)

### Google Pub/Sub Example

1. Start the Google Pub/Sub emulator and applications:
```bash
cd examples/googlepubsub
podman compose up
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

## Building

Build both commands:
```bash
make build
```

This will create:
- `bin/publisher` - Publisher executable
- `bin/subscriber` - Subscriber executable

Or build individually:
```bash
go build -o bin/publisher ./cmd/publisher
go build -o bin/subscriber ./cmd/subscriber
```

## Building Docker Images

Build Docker images:
```bash
make docker-build
```

Or manually:
```bash
docker build -t watermill-publisher:latest -f Dockerfile.publisher .
docker build -t watermill-subscriber:latest -f Dockerfile.subscriber .
```

## Requirements

- Go 1.23 or later
- Docker and Docker Compose (for examples)
- RabbitMQ (for RabbitMQ example) or Google Cloud SDK emulator (for Pub/Sub example)

## License

MIT

