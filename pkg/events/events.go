package events

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/asyncapi-cloudevents/watermill-abstraction/pkg/pubsub"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
)

// NodePoolEvent represents a node pool event with cluster and pool information
type NodePoolEvent struct {
	ClusterID  int32  `json:"clusterId"`
	ID         int32  `json:"id"`
	Href       string `json:"href"`
	Generation int32  `json:"generation"`
}

// CloudEventType is the CloudEvent type for NodePoolEvent
const CloudEventType = "com.example.nodepool.event"

// CloudEventSource is the default source for CloudEvents
const CloudEventSource = "watermill-publisher"

// NewCloudEvent converts any event data to a CloudEvent
func NewCloudEvent(source, eventType string, event any) (*cloudevents.Event, error) {
	ce := cloudevents.NewEvent()
	ce.SetType(eventType)
	if source == "" {
		source = CloudEventSource
	}
	ce.SetSource(source)
	ce.SetID(uuid.New().String())

	// Set the event data
	if err := ce.SetData(cloudevents.ApplicationJSON, event); err != nil {
		return nil, fmt.Errorf("failed to set event data: %w", err)
	}

	return &ce, nil
}

// FromCloudEvent extracts a NodePoolEvent from a CloudEvent
func FromCloudEvent(ce cloudevents.Event) (*NodePoolEvent, error) {
	var event NodePoolEvent
	if err := ce.DataAs(&event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event data: %w", err)
	}
	return &event, nil
}

// CloudEventToMessage converts a CloudEvent to a pubsub.Message
func CloudEventToMessage(ce *cloudevents.Event) (*pubsub.Message, error) {
	// Marshal CloudEvent to JSON
	bytes, err := json.Marshal(ce)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CloudEvent: %w", err)
	}

	// Extract CloudEvent attributes as metadata
	metadata := make(map[string]string)
	metadata["ce_id"] = ce.ID()
	metadata["ce_type"] = ce.Type()
	metadata["ce_source"] = ce.Source()
	if !ce.Time().IsZero() {
		metadata["ce_time"] = ce.Time().Format(time.RFC3339)
	}
	if ce.Subject() != "" {
		metadata["ce_subject"] = ce.Subject()
	}
	if ce.DataContentType() != "" {
		metadata["ce_datacontenttype"] = ce.DataContentType()
	}

	// Copy extension attributes
	for k, v := range ce.Extensions() {
		if str, ok := v.(string); ok {
			metadata["ce_extension_"+k] = str
		}
	}

	return &pubsub.Message{
		UUID:     ce.ID(),
		Payload:  bytes,
		Metadata: metadata,
	}, nil
}

// MessageToCloudEvent converts a pubsub.Message to a CloudEvent
func MessageToCloudEvent(msg *pubsub.Message) (*cloudevents.Event, error) {
	var ce cloudevents.Event
	if err := json.Unmarshal(msg.Payload, &ce); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CloudEvent: %w", err)
	}
	return &ce, nil
}
