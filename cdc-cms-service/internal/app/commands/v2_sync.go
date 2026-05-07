package commands

import (
	"context"
	"encoding/json"
	"errors"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/infra/persistence"
	"cdc-cms-service/internal/model"
)

// V2SyncCommand mirrors a freshly registered/updated V1 TableRegistry row
// into the V2 source_object_registry + shadow_binding metadata. Sync —
// two-table write inside one request, no NATS dispatch (see ADR-CMS-HEX
// "audit-only / metadata-only side-effects do not need CommandBus async
// hop").
type V2SyncCommand struct {
	ports.SyncCommandMixin
	Entry *model.TableRegistry `json:"entry"`
}

func (V2SyncCommand) Type() string { return "source.v2-sync" }

func (c V2SyncCommand) Validate() error {
	if c.Entry == nil {
		return errors.New("v2_sync_entry_nil")
	}
	if c.Entry.ID == 0 {
		return errors.New("v2_sync_entry_missing_id")
	}
	return nil
}

type V2SyncHandler struct {
	svc *persistence.SourceObjectV2SyncService
}

func NewV2SyncHandler(svc *persistence.SourceObjectV2SyncService) *V2SyncHandler {
	return &V2SyncHandler{svc: svc}
}

func (h *V2SyncHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(V2SyncCommand)
	if !ok {
		return nil, errors.New("source.v2-sync: command type mismatch")
	}
	if h.svc == nil {
		return nil, errors.New("v2 sync service not ready")
	}
	if err := h.svc.SyncFromLegacy(ctx, cmd.Entry); err != nil {
		return nil, err
	}
	body, _ := json.Marshal(map[string]any{
		"message":     "v2 metadata mirrored",
		"registry_id": cmd.Entry.ID,
	})
	return body, nil
}
