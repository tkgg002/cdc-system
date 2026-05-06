package ports

import "context"

// Publisher is the low-level fire-and-forget NATS publisher. The
// CommandBus wraps it; some legacy call-sites (activity log, schema
// reload) publish directly via this port until they are migrated.
type Publisher interface {
	Publish(ctx context.Context, subject string, payload []byte) error
}
