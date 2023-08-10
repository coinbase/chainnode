package constants

type (
	DispatchMode string
)

const (
	ServiceName                = "chainnode"
	FullServiceName            = "coinbase.chainnode.ChainNode"
	ContextKeyLatestCheckpoint = "latest_checkpoint"
	RoutingModeHttpHeaderName  = "x-chainnode-routing-mode"
	ProjectName                = "data/chainnode"

	// NativeOnlyMode - Only dispatch methods if supported by pre-indexed chain-node data. Any proxied calls will be rejected with ErrNotAllowed exception.
	// DynamicMode - The default dispatch mode where jsonrpc calls will be routed to the corresponding calls regardless if they are supported natively or by proxying to primary endpoint.
	// InvalidMode - Any "x-chainnode-routing-mode" header value other than "native-only" is considered to be invalid.
	NativeOnlyMode = DispatchMode("native-only")
	DynamicMode    = DispatchMode("dynamic")
	InvalidMode    = DispatchMode("invalid")
)
