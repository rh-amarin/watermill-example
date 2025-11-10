package events

import (
	"fmt"
	"strings"
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

// CloudEventToEventMessage converts a CloudEvent to a pubsub.EventMessage
func CloudEventToEventMessage(ce *cloudevents.Event) (*pubsub.EventMessage, error) {
	// Extract CloudEvent data
	var data any
	if err := ce.DataAs(&data); err != nil {
		// If DataAs fails, try to get raw bytes
		if bytes := ce.Data(); len(bytes) > 0 {
			data = bytes
		}
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

	return &pubsub.EventMessage{
		ID:       ce.ID(),
		Type:     ce.Type(),
		Source:   ce.Source(),
		Payload:  data,
		Metadata: metadata,
	}, nil
}

// EventMessageToCloudEvent converts a pubsub.EventMessage to a CloudEvent
func EventMessageToCloudEvent(msg *pubsub.EventMessage) (*cloudevents.Event, error) {
	ce := cloudevents.NewEvent()
	ce.SetID(msg.ID)
	ce.SetType(msg.Type)
	ce.SetSource(msg.Source)

	// Set the event data
	if err := ce.SetData(cloudevents.ApplicationJSON, msg.Payload); err != nil {
		return nil, fmt.Errorf("failed to set event data: %w", err)
	}

	// Set optional fields from metadata
	if timeStr, ok := msg.Metadata["ce_time"]; ok {
		if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
			ce.SetTime(t)
		}
	}
	if subject, ok := msg.Metadata["ce_subject"]; ok {
		ce.SetSubject(subject)
	}

	// Set extensions from metadata
	for k, v := range msg.Metadata {
		if strings.HasPrefix(k, "ce_extension_") {
			extKey := strings.TrimPrefix(k, "ce_extension_")
			ce.SetExtension(extKey, v)
		}
	}

	return &ce, nil
}
