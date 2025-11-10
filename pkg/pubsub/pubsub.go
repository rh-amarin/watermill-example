package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Publisher is an interface for publishing messages
type Publisher interface {
	Publish(ctx context.Context, topic string, event *EventMessage) error
	Close() error
}

// Subscriber is an interface for subscribing to messages
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}

// MessageHandler is a function that processes messages
type MessageHandler func(ctx context.Context, msg *EventMessage) error

// PubSub combines Publisher and Subscriber interfaces
type PubSub interface {
	Publisher
	Subscriber
}

// EventMessage represents an event message to be published
type EventMessage struct {
	ID       string            // Event ID (required)
	Type     string            // Event type (required)
	Source   string            // Event source (required)
	Payload  any               // Event data payload
	Metadata map[string]string // Additional metadata/extensions
}

// CloudEvent represents a CloudEvents-compliant event structure
// Following the CloudEvents JSON specification: https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
type CloudEvent struct {
	SpecVersion     string                 `json:"specversion"`               // Required: "1.0"
	Type            string                 `json:"type"`                      // Required
	Source          string                 `json:"source"`                    // Required
	ID              string                 `json:"id"`                        // Required
	Time            *time.Time             `json:"time,omitempty"`            // Optional: RFC3339 timestamp
	DataContentType string                 `json:"datacontenttype,omitempty"` // Optional: e.g., "application/json"
	DataSchema      string                 `json:"dataschema,omitempty"`      // Optional
	Subject         string                 `json:"subject,omitempty"`         // Optional
	Data            any                    `json:"data,omitempty"`            // Optional: the event payload
	Extensions      map[string]interface{} `json:"-"`                         // Extensions (will be merged into root)
}

// watermillMessageToEventMessage converts a watermill message directly to an EventMessage by parsing the CloudEvent JSON
func watermillMessageToEventMessage(msg *message.Message) (*EventMessage, error) {
	// Parse CloudEvent from JSON payload
	ce, err := ParseCloudEventFromJSON(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CloudEvent from JSON: %w", err)
	}

	// Extract metadata from watermill message metadata and CloudEvent extensions
	metadata := make(map[string]string)
	// Copy metadata from watermill message
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	// Add CloudEvent extensions to metadata
	if ce.Extensions != nil {
		for k, v := range ce.Extensions {
			if str, ok := v.(string); ok {
				metadata[k] = str
			}
		}
	}

	eventMsg := &EventMessage{
		ID:       ce.ID,
		Type:     ce.Type,
		Source:   ce.Source,
		Payload:  ce.Data,
		Metadata: metadata,
	}

	return eventMsg, nil
}

// eventMessageToCloudEvent converts an EventMessage to a CloudEvent struct
func eventMessageToCloudEvent(event *EventMessage) (*CloudEvent, error) {
	if event.ID == "" {
		return nil, fmt.Errorf("event ID is required")
	}
	if event.Type == "" {
		return nil, fmt.Errorf("event type is required")
	}
	if event.Source == "" {
		return nil, fmt.Errorf("event source is required")
	}

	now := time.Now()
	ce := &CloudEvent{
		SpecVersion:     "1.0",
		Type:            event.Type,
		Source:          event.Source,
		ID:              event.ID,
		Time:            &now,
		DataContentType: "application/json",
		Data:            event.Payload,
		Extensions:      make(map[string]interface{}),
	}

	// Copy metadata as extensions
	if event.Metadata != nil {
		for k, v := range event.Metadata {
			ce.Extensions[k] = v
		}
	}

	return ce, nil
}

// cloudEventToJSON marshals a CloudEvent to JSON following CloudEvents spec
func cloudEventToJSON(ce *CloudEvent) ([]byte, error) {
	// Create a map to hold the CloudEvent structure
	eventMap := make(map[string]interface{})

	// Required fields
	eventMap["specversion"] = ce.SpecVersion
	eventMap["type"] = ce.Type
	eventMap["source"] = ce.Source
	eventMap["id"] = ce.ID

	// Optional fields
	if ce.Time != nil {
		eventMap["time"] = ce.Time.Format(time.RFC3339)
	}
	if ce.DataContentType != "" {
		eventMap["datacontenttype"] = ce.DataContentType
	}
	if ce.DataSchema != "" {
		eventMap["dataschema"] = ce.DataSchema
	}
	if ce.Subject != "" {
		eventMap["subject"] = ce.Subject
	}
	if ce.Data != nil {
		eventMap["data"] = ce.Data
	}

	// Add extensions to the root level
	for k, v := range ce.Extensions {
		eventMap[k] = v
	}

	return json.Marshal(eventMap)
}

// eventMessageToWatermillMessage converts an EventMessage to a watermill message
func eventMessageToWatermillMessage(event *EventMessage) (*message.Message, error) {
	// Convert EventMessage to CloudEvent
	ce, err := eventMessageToCloudEvent(event)
	if err != nil {
		return nil, fmt.Errorf("failed to create CloudEvent: %w", err)
	}

	// Marshal CloudEvent to JSON
	bytes, err := cloudEventToJSON(ce)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CloudEvent: %w", err)
	}

	// Create watermill message with Event ID as UUID
	watermillMsg := message.NewMessage(event.ID, bytes)

	return watermillMsg, nil
}

// ParseCloudEventFromJSON parses a CloudEvent from JSON bytes
func ParseCloudEventFromJSON(data []byte) (*CloudEvent, error) {
	var eventMap map[string]interface{}
	if err := json.Unmarshal(data, &eventMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CloudEvent JSON: %w", err)
	}

	ce := &CloudEvent{
		Extensions: make(map[string]interface{}),
	}

	// Extract required fields
	if specversion, ok := eventMap["specversion"].(string); ok {
		ce.SpecVersion = specversion
	}
	if eventType, ok := eventMap["type"].(string); ok {
		ce.Type = eventType
	}
	if source, ok := eventMap["source"].(string); ok {
		ce.Source = source
	}
	if id, ok := eventMap["id"].(string); ok {
		ce.ID = id
	}

	// Extract optional fields
	if timeStr, ok := eventMap["time"].(string); ok {
		if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			ce.Time = &t
		}
	}
	if datacontenttype, ok := eventMap["datacontenttype"].(string); ok {
		ce.DataContentType = datacontenttype
	}
	if dataschema, ok := eventMap["dataschema"].(string); ok {
		ce.DataSchema = dataschema
	}
	if subject, ok := eventMap["subject"].(string); ok {
		ce.Subject = subject
	}
	if data, ok := eventMap["data"]; ok {
		ce.Data = data
	}

	// Extract extensions (all fields that are not standard CloudEvent attributes)
	standardFields := map[string]bool{
		"specversion":     true,
		"type":            true,
		"source":          true,
		"id":              true,
		"time":            true,
		"datacontenttype": true,
		"dataschema":      true,
		"subject":         true,
		"data":            true,
	}

	for k, v := range eventMap {
		if !standardFields[k] {
			ce.Extensions[k] = v
		}
	}

	return ce, nil
}

// BrokerType represents the type of message broker
type BrokerType string

const (
	BrokerTypeRabbitMQ     BrokerType = "rabbitmq"
	BrokerTypeGooglePubSub BrokerType = "googlepubsub"
)

// Config holds configuration for creating a PubSub instance
type Config struct {
	BrokerType BrokerType
	Logger     watermill.LoggerAdapter

	// RabbitMQ specific config
	RabbitMQURL string

	// Google Pub/Sub specific config
	GoogleProjectID       string
	GoogleCredentialsPath string
}

// NewPubSub creates a new PubSub instance based on the broker type
func NewPubSub(config Config) (PubSub, error) {
	switch config.BrokerType {
	case BrokerTypeRabbitMQ:
		return NewRabbitMQPubSub(config)
	case BrokerTypeGooglePubSub:
		return NewGooglePubSubPubSub(config)
	default:
		return nil, ErrUnsupportedBrokerType
	}
}
