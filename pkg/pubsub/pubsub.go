package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// EventMessage represents an event message with a typed payload
type EventMessage[T any] struct {
	ID       string            // Event ID (required)
	Type     string            // Event type (required)
	Source   string            // Event source (required)
	Payload  T                 // Typed event data payload
	Metadata map[string]string // Additional metadata/extensions
}

// MessageHandler is a generic function that processes messages with typed payloads
type MessageHandler[T any] func(ctx context.Context, msg *EventMessage[T]) error

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

// eventMessageToCloudEvent converts a typed EventMessage to a CloudEvent struct
func eventMessageToCloudEvent[T any](event *EventMessage[T]) (*CloudEvent, error) {
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

// eventMessageToWatermillMessage converts a typed EventMessage to a watermill message
func eventMessageToWatermillMessage[T any](event *EventMessage[T]) (*message.Message, error) {
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

// parseCloudEventWithTypedData unmarshals CloudEvent JSON and extracts data directly into type T
// This uses a single JSON unmarshal by leveraging json.RawMessage to avoid double marshalling
func parseCloudEventWithTypedData[T any](data []byte) (*EventMessage[T], error) {
	// Use a struct that captures the CloudEvent structure with data as RawMessage
	type cloudEventJSON struct {
		SpecVersion     string          `json:"specversion"`
		Type            string          `json:"type"`
		Source          string          `json:"source"`
		ID              string          `json:"id"`
		Time            *string         `json:"time,omitempty"`
		DataContentType string          `json:"datacontenttype,omitempty"`
		DataSchema      string          `json:"dataschema,omitempty"`
		Subject         string          `json:"subject,omitempty"`
		Data            json.RawMessage `json:"data,omitempty"` // Keep as raw JSON
	}

	var ceJSON cloudEventJSON

	// First pass: unmarshal everything including data as RawMessage
	if err := json.Unmarshal(data, &ceJSON); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CloudEvent JSON: %w", err)
	}

	// Extract extensions (fields not in standard struct)
	var fullMap map[string]interface{}
	if err := json.Unmarshal(data, &fullMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CloudEvent for extensions: %w", err)
	}

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

	extensions := make(map[string]interface{})
	for k, v := range fullMap {
		if !standardFields[k] {
			extensions[k] = v
		}
	}

	// Extract metadata from extensions
	metadata := make(map[string]string)
	for k, v := range extensions {
		if str, ok := v.(string); ok {
			metadata[k] = str
		}
	}

	// Now unmarshal the data field directly into type T (single unmarshal, no double marshalling!)
	var payload T
	if len(ceJSON.Data) > 0 {
		if err := json.Unmarshal(ceJSON.Data, &payload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal CloudEvent data to type: %w", err)
		}
	}

	return &EventMessage[T]{
		ID:       ceJSON.ID,
		Type:     ceJSON.Type,
		Source:   ceJSON.Source,
		Payload:  payload,
		Metadata: metadata,
	}, nil
}

// watermillMessageToEventMessage converts a watermill message to an EventMessage
// This avoids double marshalling by using json.RawMessage
func watermillMessageToEventMessage[T any](msg *message.Message) (*EventMessage[T], error) {
	// Parse CloudEvent with typed data extraction
	eventMsg, err := parseCloudEventWithTypedData[T](msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CloudEvent with typed data: %w", err)
	}

	// Merge watermill message metadata with CloudEvent extensions
	for k, v := range msg.Metadata {
		if eventMsg.Metadata == nil {
			eventMsg.Metadata = make(map[string]string)
		}
		eventMsg.Metadata[k] = v
	}

	return eventMsg, nil
}

// RabbitMQConfig holds configuration for RabbitMQ Pub/Sub
type RabbitMQConfig struct {
	Logger         watermill.LoggerAdapter
	URL            string
	WorkerPoolSize int
}

// GooglePubSubConfig holds configuration for Google Cloud Pub/Sub
type GooglePubSubConfig struct {
	Logger                   watermill.LoggerAdapter
	ProjectID                string
	CredentialsPath          string
	WorkerPoolSize           int
	GenerateSubscriptionName func(topic string) string
}

// messageJob represents a job to be processed by a worker
type messageJob[T any] struct {
	msg     *message.Message
	handler MessageHandler[T]
	ctx     context.Context
	logger  watermill.LoggerAdapter
	topic   string
}

// workerPool manages a pool of workers for processing messages
type workerPool[T any] struct {
	workers int
	jobChan chan messageJob[T]
	logger  watermill.LoggerAdapter
	topic   string
	wg      sync.WaitGroup
}

// newWorkerPool creates a new worker pool
func newWorkerPool[T any](workers int, logger watermill.LoggerAdapter, topic string) *workerPool[T] {
	if workers <= 0 {
		workers = 1
	}
	return &workerPool[T]{
		workers: workers,
		jobChan: make(chan messageJob[T], workers*2), // Buffer size: 2x workers
		logger:  logger,
		topic:   topic,
	}
}

// start starts the worker pool
func (wp *workerPool[T]) start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

// worker processes messages from the job channel
func (wp *workerPool[T]) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			wp.logger.Info("worker shutting down", watermill.LogFields{
				"worker_id": id,
				"topic":     wp.topic,
			})
			return
		case job, ok := <-wp.jobChan:
			if !ok {
				// Channel closed, exit
				return
			}
			wp.processJob(ctx, job, id)
		}
	}
}

// processJob processes a single message job with panic recovery
func (wp *workerPool[T]) processJob(ctx context.Context, job messageJob[T], workerID int) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			wp.logger.Error("panic recovered in worker", fmt.Errorf("panic: %v", r), watermill.LogFields{
				"worker_id":  workerID,
				"topic":      wp.topic,
				"message_id": job.msg.UUID,
				"stack":      string(stack),
			})
			job.msg.Nack()
		}
	}()

	// Convert watermill message to EventMessage (avoids double marshalling)
	eventMsg, err := watermillMessageToEventMessage[T](job.msg)
	if err != nil {
		wp.logger.Error("failed to convert message to EventMessage", err, watermill.LogFields{
			"worker_id":  workerID,
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
		return
	}

	// Wrap handler with panic recovery
	safeHandler := func(ctx context.Context, msg *EventMessage[T]) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				wp.logger.Error("panic recovered in message handler", fmt.Errorf("panic: %v", r), watermill.LogFields{
					"topic":      wp.topic,
					"message_id": job.msg.UUID,
					"stack":      string(stack),
				})
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return job.handler(ctx, msg)
	}

	// Process message using the job's context
	if err := safeHandler(job.ctx, eventMsg); err != nil {
		wp.logger.Error("failed to handle message", err, watermill.LogFields{
			"worker_id":  workerID,
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
		return
	}

	job.msg.Ack()
}

// submit submits a job to the worker pool
func (wp *workerPool[T]) submit(job messageJob[T]) {
	select {
	case wp.jobChan <- job:
		// Job submitted successfully
	default:
		wp.logger.Error("worker pool job channel full, dropping message", nil, watermill.LogFields{
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
	}
}

// stop stops the worker pool gracefully
func (wp *workerPool[T]) stop() {
	close(wp.jobChan)
	wp.wg.Wait()
}
