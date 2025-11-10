package events

// NodePoolEvent represents a node pool event with cluster and pool information
type NodePoolEvent struct {
	ClusterID  int32  `json:"clusterId"`
	ID         int32  `json:"id"`
	Href       string `json:"href"`
	Generation int32  `json:"generation"`
}

// CloudEventType is the CloudEvent type for NodePoolEvent
const CloudEventType = "com.example.nodepool.event"
