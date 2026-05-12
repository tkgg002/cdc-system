package commands

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
	"cdc-cms-service/pkgs/natsconn"
)

// ShadowTableEnsurer abstracts the shadow-DDL provisioner so the
// commands package doesn't import service. ShadowAutomator already
// satisfies it implicitly via its EnsureShadowTable method.
type ShadowTableEnsurer interface {
	EnsureShadowTable(ctx context.Context, reg *model.TableRegistry, shadowSchema string) error
}

// SourceObjectSyncer abstracts the V2 synchronization logic.
type SourceObjectSyncer interface {
	SyncFromLegacyTx(ctx context.Context, tx *gorm.DB, entry *model.TableRegistry) error
}

// RegisterRegistryCommand wraps the atomic part of the legacy Register
// flow: INSERT registry row → ensure shadow table → rollback row on
// shadow DDL failure → emit reload + activity log. Cascading async
// dispatches (CreateDefaultColumnsCommand) and v2 sync stay at the API
// layer (post-bus) — they're idempotent and not destructive.
type RegisterRegistryCommand struct {
	ports.SyncCommandMixin
	Entry     model.TableRegistry `json:"entry"`
	CreatedBy string              `json:"created_by,omitempty"`
}

func (RegisterRegistryCommand) Type() string { return "registry.register" }

var ErrShadowDDLFailed = errors.New("shadow_ddl_failed")

func (c RegisterRegistryCommand) Validate() error {
	if c.Entry.SourceDB == "" || c.Entry.SourceTable == "" || c.Entry.TargetTable == "" {
		return errors.New("source_db, source_table, target_table required")
	}
	if c.Entry.PrimaryKeyField == "" {
		return errors.New("primary_key_field required")
	}
	return nil
}

type RegisterRegistryHandler struct {
	db        *gorm.DB
	automator ShadowTableEnsurer
	syncer    SourceObjectSyncer
	nats      *natsconn.NatsClient
	logger    *zap.Logger
}

func NewRegisterRegistryHandler(
	db *gorm.DB,
	automator ShadowTableEnsurer,
	syncer SourceObjectSyncer,
	nats *natsconn.NatsClient,
	logger *zap.Logger,
) *RegisterRegistryHandler {
	return &RegisterRegistryHandler{db: db, automator: automator, syncer: syncer, nats: nats, logger: logger}
}

func (h *RegisterRegistryHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(RegisterRegistryCommand)
	if !ok {
		return nil, errors.New("registry.register: command type mismatch")
	}
	if h.db == nil {
		return nil, errors.New("registry store not ready")
	}

	entry := cmd.Entry
	entry.PrimaryKeyType = normalizePKType(entry.PrimaryKeyType)

	// Perform registration in a transaction to ensure V1 and V2 consistency
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&entry).Error; err != nil {
			return err
		}

		if h.syncer != nil {
			if err := h.syncer.SyncFromLegacyTx(ctx, tx, &entry); err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if h.automator != nil {
		shadowSchema := "shadow_" + normalizeShadowIdent(entry.SourceDB)
		if err := h.automator.EnsureShadowTable(ctx, &entry, shadowSchema); err != nil {
			if delErr := h.db.WithContext(ctx).Delete(&model.TableRegistry{}, entry.ID).Error; delErr != nil {
				h.logger.Error("registry rollback failed after shadow err",
					zap.Uint("id", entry.ID), zap.Error(delErr))
			}
			return nil, errors.Join(ErrShadowDDLFailed, err)
		}
	}

	if h.nats != nil {
		_ = h.nats.PublishReload(entry.TargetTable, cmd.CreatedBy, "register", "")
	}

	now := time.Now()
	details, _ := json.Marshal(map[string]interface{}{
		"user": cmd.CreatedBy,
	})
	if err := h.db.WithContext(ctx).Create(&model.ActivityLog{
		Operation:   "register",
		TargetTable: entry.TargetTable,
		Status:      "accepted",
		Details:     details,
		TriggeredBy: "manual",
		StartedAt:   now,
		CompletedAt: &now,
	}).Error; err != nil {
		h.logger.Warn("register activity log failed", zap.Error(err))
	}

	body, _ := json.Marshal(map[string]interface{}{
		"message": "table registered",
		"entry":   entry,
	})
	return body, nil
}

// normalizePKType maps Mongo/BSON-flavored primary-key type names that
// PostgreSQL doesn't recognize onto canonical PG types. Worker
// command_handler propagates pk_type into ALTER/CREATE DDL verbatim, so a
// raw "string" value triggers SQLSTATE 42704. Narrow scope: only the
// observed Flow-1 case ("string" → "text"); other unknown values pass
// through so worker validation still surfaces them.
func normalizePKType(t string) string {
	if strings.EqualFold(strings.TrimSpace(t), "string") {
		return "text"
	}
	return t
}

// normalizeShadowIdent — duplicated here to keep commands package free of
// any api-package import. Mirrors api.normalizeShadowIdent byte-for-byte.
func normalizeShadowIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z':
			out = append(out, c+32)
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
			out = append(out, c)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
