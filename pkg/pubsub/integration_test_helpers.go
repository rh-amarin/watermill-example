package pubsub

// NodePoolEvent represents a node pool event for testing
// Exported for use in integration test sub-packages
type NodePoolEvent struct {
	ClusterID  int32  `json:"clusterId"`
	ID         int32  `json:"id"`
	Href       string `json:"href"`
	Generation int32  `json:"generation"`
}

const (
	// CloudEventType is the CloudEvent type used in integration tests
	CloudEventType = "com.example.nodepool.event"
	// CloudEventSource is the CloudEvent source used in integration tests
	CloudEventSource = "watermill-publisher"
)


