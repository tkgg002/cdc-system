// Package persistence — wizard_repo_gorm.go is the GORM-backed adapter
// for ports.WizardRepo. SQL is lifted verbatim from the legacy
// `internal/repository/wizard_repo.go` so the row shape, JSONB
// defaults and append-progress semantics stay byte-identical after
// Task #19 đợt C.
package persistence

import (
	"context"
	"encoding/json"
	"time"

	"cdc-cms-service/internal/app/ports"
	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type wizardRepoGorm struct {
	db *gorm.DB
}

// NewWizardRepo constructs the GORM-backed adapter for ports.WizardRepo.
func NewWizardRepo(db *gorm.DB) ports.WizardRepo {
	return &wizardRepoGorm{db: db}
}

func (r *wizardRepoGorm) Create(ctx context.Context, s *model.WizardSession) error {
	if len(s.StepPayload) == 0 {
		s.StepPayload = []byte("{}")
	}
	if len(s.ProgressLog) == 0 {
		s.ProgressLog = []byte("[]")
	}
	return r.db.WithContext(ctx).Create(s).Error
}

func (r *wizardRepoGorm) Get(ctx context.Context, id string) (*model.WizardSession, error) {
	var s model.WizardSession
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&s).Error
	return &s, err
}

func (r *wizardRepoGorm) Update(ctx context.Context, id string, updates map[string]interface{}) error {
	updates["updated_at"] = time.Now()
	return r.db.WithContext(ctx).Model(&model.WizardSession{}).
		Where("id = ?", id).
		Updates(updates).Error
}

func (r *wizardRepoGorm) AppendProgress(ctx context.Context, id string, entry map[string]interface{}) error {
	entry["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return r.db.WithContext(ctx).Exec(
		`UPDATE cdc_system.cdc_wizard_sessions
		    SET progress_log = progress_log || ?::jsonb,
		        updated_at = NOW()
		  WHERE id = ?`,
		string(raw), id,
	).Error
}
