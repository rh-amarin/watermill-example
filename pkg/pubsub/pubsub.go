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

// TypedEventMessage represents an event message with a typed payload
type TypedEventMessage[T any] struct {
	ID       string            // Event ID (required)
	Type     string            // Event type (required)
	Source   string            // Event source (required)
	Payload  T                 // Typed event data payload
	Metadata map[string]string // Additional metadata/extensions
}

// TypedMessageHandler is a generic function that processes messages with typed payloads
type TypedMessageHandler[T any] func(ctx context.Context, msg *TypedEventMessage[T]) error

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

// parseCloudEventWithTypedData unmarshals CloudEvent JSON and extracts data directly into type T
// This uses a single JSON unmarshal by leveraging json.RawMessage to avoid double marshalling
func parseCloudEventWithTypedData[T any](data []byte) (*TypedEventMessage[T], error) {
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

	return &TypedEventMessage[T]{
		ID:       ceJSON.ID,
		Type:     ceJSON.Type,
		Source:   ceJSON.Source,
		Payload:  payload,
		Metadata: metadata,
	}, nil
}

// watermillMessageToTypedEventMessage converts a watermill message to a TypedEventMessage
// This avoids double marshalling by using json.RawMessage
func watermillMessageToTypedEventMessage[T any](msg *message.Message) (*TypedEventMessage[T], error) {
	// Parse CloudEvent with typed data extraction
	typedMsg, err := parseCloudEventWithTypedData[T](msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CloudEvent with typed data: %w", err)
	}

	// Merge watermill message metadata with CloudEvent extensions
	for k, v := range msg.Metadata {
		if typedMsg.Metadata == nil {
			typedMsg.Metadata = make(map[string]string)
		}
		typedMsg.Metadata[k] = v
	}

	return typedMsg, nil
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

// withPanicRecovery wraps a message handler with panic recovery
func withPanicRecovery(handler MessageHandler, logger watermill.LoggerAdapter, topic, messageID string) MessageHandler {
	return func(ctx context.Context, msg *EventMessage) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				logger.Error("panic recovered in message handler", fmt.Errorf("panic: %v", r), watermill.LogFields{
					"topic":      topic,
					"message_id": messageID,
					"stack":      string(stack),
				})
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return handler(ctx, msg)
	}
}

// messageJob represents a job to be processed by a worker
type messageJob struct {
	msg     *message.Message
	handler MessageHandler
	ctx     context.Context
	logger  watermill.LoggerAdapter
	topic   string
}

// workerPool manages a pool of workers for processing messages
type workerPool struct {
	workers int
	jobChan chan messageJob
	logger  watermill.LoggerAdapter
	topic   string
	wg      sync.WaitGroup
}

// newWorkerPool creates a new worker pool
func newWorkerPool(workers int, logger watermill.LoggerAdapter, topic string) *workerPool {
	if workers <= 0 {
		workers = 1
	}
	return &workerPool{
		workers: workers,
		jobChan: make(chan messageJob, workers*2), // Buffer size: 2x workers
		logger:  logger,
		topic:   topic,
	}
}

// start starts the worker pool
func (wp *workerPool) start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

// worker processes messages from the job channel
func (wp *workerPool) worker(ctx context.Context, id int) {
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
func (wp *workerPool) processJob(ctx context.Context, job messageJob, workerID int) {
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

	// Convert watermill message to EventMessage
	eventMsg, err := watermillMessageToEventMessage(job.msg)
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
	safeHandler := withPanicRecovery(job.handler, wp.logger, wp.topic, job.msg.UUID)

	// Process message using the job's context (from Subscribe)
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
func (wp *workerPool) submit(job messageJob) {
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
func (wp *workerPool) stop() {
	close(wp.jobChan)
	wp.wg.Wait()
}

// typedMessageJob represents a job to be processed by a typed worker
type typedMessageJob[T any] struct {
	msg     *message.Message
	handler TypedMessageHandler[T]
	ctx     context.Context
	logger  watermill.LoggerAdapter
	topic   string
}

// typedWorkerPool manages a pool of workers for processing typed messages
type typedWorkerPool[T any] struct {
	workers int
	jobChan chan typedMessageJob[T]
	logger  watermill.LoggerAdapter
	topic   string
	wg      sync.WaitGroup
}

// newTypedWorkerPool creates a new typed worker pool
func newTypedWorkerPool[T any](workers int, logger watermill.LoggerAdapter, topic string) *typedWorkerPool[T] {
	if workers <= 0 {
		workers = 1
	}
	return &typedWorkerPool[T]{
		workers: workers,
		jobChan: make(chan typedMessageJob[T], workers*2), // Buffer size: 2x workers
		logger:  logger,
		topic:   topic,
	}
}

// start starts the typed worker pool
func (wp *typedWorkerPool[T]) start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(ctx, i)
	}
}

// worker processes typed messages from the job channel
func (wp *typedWorkerPool[T]) worker(ctx context.Context, id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-ctx.Done():
			wp.logger.Info("typed worker shutting down", watermill.LogFields{
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

// processJob processes a single typed message job with panic recovery
func (wp *typedWorkerPool[T]) processJob(ctx context.Context, job typedMessageJob[T], workerID int) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			wp.logger.Error("panic recovered in typed worker", fmt.Errorf("panic: %v", r), watermill.LogFields{
				"worker_id":  workerID,
				"topic":      wp.topic,
				"message_id": job.msg.UUID,
				"stack":      string(stack),
			})
			job.msg.Nack()
		}
	}()

	// Convert watermill message to TypedEventMessage (avoids double marshalling)
	typedMsg, err := watermillMessageToTypedEventMessage[T](job.msg)
	if err != nil {
		wp.logger.Error("failed to convert message to TypedEventMessage", err, watermill.LogFields{
			"worker_id":  workerID,
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
		return
	}

	// Wrap handler with panic recovery
	safeHandler := func(ctx context.Context, msg *TypedEventMessage[T]) (err error) {
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				wp.logger.Error("panic recovered in typed message handler", fmt.Errorf("panic: %v", r), watermill.LogFields{
					"topic":      wp.topic,
					"message_id": job.msg.UUID,
					"stack":      string(stack),
				})
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return job.handler(ctx, msg)
	}

	// Process message using the job's context (from SubscribeTyped)
	if err := safeHandler(job.ctx, typedMsg); err != nil {
		wp.logger.Error("failed to handle typed message", err, watermill.LogFields{
			"worker_id":  workerID,
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
		return
	}

	job.msg.Ack()
}

// submit submits a typed job to the worker pool
func (wp *typedWorkerPool[T]) submit(job typedMessageJob[T]) {
	select {
	case wp.jobChan <- job:
		// Job submitted successfully
	default:
		wp.logger.Error("typed worker pool job channel full, dropping message", nil, watermill.LogFields{
			"topic":      wp.topic,
			"message_id": job.msg.UUID,
		})
		job.msg.Nack()
	}
}

// stop stops the typed worker pool gracefully
func (wp *typedWorkerPool[T]) stop() {
	close(wp.jobChan)
	wp.wg.Wait()
}
