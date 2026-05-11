package commands

import (
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/zap"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"
)

// KafkaConnectorWriter is the narrow interface this package needs from
// the Kafka Connect REST client. Concrete *infrahttp.KafkaConnectClient
// satisfies it via Go structural typing — keeps commands package free
// of the infra/http dependency.
type KafkaConnectorWriter interface {
	GetConfig(ctx context.Context, name string) (map[string]string, error)
	Create(ctx context.Context, name string, cfg map[string]string) (map[string]any, error)
	UpdateConfig(ctx context.Context, name string, cfg map[string]string) (map[string]any, error)
	Delete(ctx context.Context, name string) error
	Restart(ctx context.Context, name string) error
	RestartTask(ctx context.Context, name, taskID string) error
	Lifecycle(ctx context.Context, name, op string) error
}

// SourceFingerprintRepo is the narrow interface for the source-fingerprint
// audit table (CMS keeps a row per connector for the registry dropdown).
// Both sides are best-effort — connector lifecycle is the source of truth.
type SourceFingerprintRepo interface {
	Upsert(ctx context.Context, src *model.Source) error
	MarkDeleted(ctx context.Context, name string) error
}

// ── Create ─────────────────────────────────────────────────────────────

type CreateSystemConnectorCommand struct {
	ports.SyncCommandMixin
	Name        string            `json:"name"`
	Config      map[string]string `json:"config"`
	Fingerprint *model.Source     `json:"fingerprint,omitempty"`
	CreatedBy   string            `json:"created_by,omitempty"`
}

func (CreateSystemConnectorCommand) Type() string { return "system-connector.create" }

func (c CreateSystemConnectorCommand) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	if len(c.Config) == 0 {
		return errors.New("config required")
	}
	return nil
}

type CreateSystemConnectorHandler struct {
	writer     KafkaConnectorWriter
	sourceRepo SourceFingerprintRepo
	logger     *zap.Logger
}

func NewCreateSystemConnectorHandler(w KafkaConnectorWriter, repo SourceFingerprintRepo, logger *zap.Logger) *CreateSystemConnectorHandler {
	return &CreateSystemConnectorHandler{writer: w, sourceRepo: repo, logger: logger}
}

func (h *CreateSystemConnectorHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(CreateSystemConnectorCommand)
	if !ok {
		return nil, errors.New("system-connector.create: command type mismatch")
	}
	if h.writer == nil {
		return nil, errors.New("kafka connect client not ready")
	}
	resp, err := h.writer.Create(ctx, cmd.Name, cmd.Config)
	if err != nil {
		return nil, err
	}
	if h.sourceRepo != nil && cmd.Fingerprint != nil {
		if uerr := h.sourceRepo.Upsert(ctx, cmd.Fingerprint); uerr != nil && h.logger != nil {
			h.logger.Warn("source fingerprint persist failed",
				zap.String("connector", cmd.Name), zap.Error(uerr))
		}
	}
	body, _ := json.Marshal(resp)
	return body, nil
}

// ── Delete ─────────────────────────────────────────────────────────────

type DeleteSystemConnectorCommand struct {
	ports.SyncCommandMixin
	Name      string `json:"name"`
	DeletedBy string `json:"deleted_by,omitempty"`
}

func (DeleteSystemConnectorCommand) Type() string { return "system-connector.delete" }

func (c DeleteSystemConnectorCommand) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	return nil
}

type DeleteSystemConnectorHandler struct {
	writer     KafkaConnectorWriter
	sourceRepo SourceFingerprintRepo
	logger     *zap.Logger
}

func NewDeleteSystemConnectorHandler(w KafkaConnectorWriter, repo SourceFingerprintRepo, logger *zap.Logger) *DeleteSystemConnectorHandler {
	return &DeleteSystemConnectorHandler{writer: w, sourceRepo: repo, logger: logger}
}

func (h *DeleteSystemConnectorHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(DeleteSystemConnectorCommand)
	if !ok {
		return nil, errors.New("system-connector.delete: command type mismatch")
	}
	if h.writer == nil {
		return nil, errors.New("kafka connect client not ready")
	}
	if err := h.writer.Delete(ctx, cmd.Name); err != nil {
		return nil, err
	}
	if h.sourceRepo != nil {
		if derr := h.sourceRepo.MarkDeleted(ctx, cmd.Name); derr != nil && h.logger != nil {
			h.logger.Warn("source soft-delete failed",
				zap.String("connector", cmd.Name), zap.Error(derr))
		}
	}
	body, _ := json.Marshal(map[string]string{"connector": cmd.Name, "status": "delete_triggered"})
	return body, nil
}

// ── Lifecycle (pause / resume / restart / restart-task) ────────────────

type LifecycleSystemConnectorCommand struct {
	ports.SyncCommandMixin
	Name      string `json:"name"`
	Operation string `json:"operation"`
	TaskID    string `json:"task_id,omitempty"`
	Actor     string `json:"actor,omitempty"`
}

func (LifecycleSystemConnectorCommand) Type() string { return "system-connector.lifecycle" }

func (c LifecycleSystemConnectorCommand) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	switch c.Operation {
	case "restart", "restart-task", "pause", "resume":
	default:
		return ErrSystemConnectorInvalidOperation
	}
	if c.Operation == "restart-task" && c.TaskID == "" {
		return errors.New("task_id required for restart-task")
	}
	return nil
}

var ErrSystemConnectorInvalidOperation = errors.New("system_connector_invalid_operation")

type LifecycleSystemConnectorHandler struct {
	writer KafkaConnectorWriter
	logger *zap.Logger
}

func NewLifecycleSystemConnectorHandler(w KafkaConnectorWriter, logger *zap.Logger) *LifecycleSystemConnectorHandler {
	return &LifecycleSystemConnectorHandler{writer: w, logger: logger}
}

func (h *LifecycleSystemConnectorHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(LifecycleSystemConnectorCommand)
	if !ok {
		return nil, errors.New("system-connector.lifecycle: command type mismatch")
	}
	if h.writer == nil {
		return nil, errors.New("kafka connect client not ready")
	}
	switch cmd.Operation {
	case "restart":
		if err := h.writer.Restart(ctx, cmd.Name); err != nil {
			return nil, err
		}
	case "restart-task":
		if err := h.writer.RestartTask(ctx, cmd.Name, cmd.TaskID); err != nil {
			return nil, err
		}
	case "pause", "resume":
		if err := h.writer.Lifecycle(ctx, cmd.Name, cmd.Operation); err != nil {
			return nil, err
		}
	default:
		return nil, ErrSystemConnectorInvalidOperation
	}
	body, _ := json.Marshal(map[string]string{
		"connector": cmd.Name,
		"operation": cmd.Operation,
		"task_id":   cmd.TaskID,
		"status":    cmd.Operation + "_triggered",
	})
	return body, nil
}

// ── Update Config ──────────────────────────────────────────────────────

const KeepSecretSentinel = "__KEEP__"

type UpdateSystemConnectorConfigCommand struct {
	ports.SyncCommandMixin
	Name        string            `json:"name"`
	Config      map[string]string `json:"config"`
	Fingerprint *model.Source     `json:"fingerprint,omitempty"`
	UpdatedBy   string            `json:"updated_by,omitempty"`
}

func (UpdateSystemConnectorConfigCommand) Type() string { return "system-connector.update-config" }

func (c UpdateSystemConnectorConfigCommand) Validate() error {
	if c.Name == "" {
		return errors.New("name required")
	}
	if len(c.Config) == 0 {
		return errors.New("config required")
	}
	return nil
}

type UpdateSystemConnectorConfigHandler struct {
	writer     KafkaConnectorWriter
	sourceRepo SourceFingerprintRepo
	logger     *zap.Logger
}

func NewUpdateSystemConnectorConfigHandler(w KafkaConnectorWriter, repo SourceFingerprintRepo, logger *zap.Logger) *UpdateSystemConnectorConfigHandler {
	return &UpdateSystemConnectorConfigHandler{writer: w, sourceRepo: repo, logger: logger}
}

func (h *UpdateSystemConnectorConfigHandler) Handle(ctx context.Context, c ports.Command) (json.RawMessage, error) {
	cmd, ok := c.(UpdateSystemConnectorConfigCommand)
	if !ok {
		return nil, errors.New("system-connector.update-config: command type mismatch")
	}
	if h.writer == nil {
		return nil, errors.New("kafka connect client not ready")
	}

	current, err := h.writer.GetConfig(ctx, cmd.Name)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]string, len(current)+len(cmd.Config))
	for k, v := range current {
		merged[k] = v
	}
	for k, v := range cmd.Config {
		if v == KeepSecretSentinel {
			continue
		}
		merged[k] = v
	}

	resp, err := h.writer.UpdateConfig(ctx, cmd.Name, merged)
	if err != nil {
		return nil, err
	}
	if h.sourceRepo != nil && cmd.Fingerprint != nil {
		if uerr := h.sourceRepo.Upsert(ctx, cmd.Fingerprint); uerr != nil && h.logger != nil {
			h.logger.Warn("source fingerprint update failed",
				zap.String("connector", cmd.Name), zap.Error(uerr))
		}
	}
	body, _ := json.Marshal(resp)
	return body, nil
}
