package nsq

type ChannelStats struct {
	ChannelName   string `json:"channel_name"`
	Depth         int64  `json:"depth"`
	BackendDepth  int64  `json:"backend_depth"`
	InFlightCount int    `json:"in_flight_count"`
	DeferredCount int    `json:"deferred_count"`
	MessageCount  uint64 `json:"message_count"`
	RequeueCount  uint64 `json:"requeue_count"`
	TimeoutCount  uint64 `json:"timeout_count"`
	Paused        bool   `json:"paused"`
}
