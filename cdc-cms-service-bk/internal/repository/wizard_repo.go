package repository

import (
	"context"
	"encoding/json"
	"time"

	"cdc-cms-service/internal/model"

	"gorm.io/gorm"
)

type WizardRepo struct{ db *gorm.DB }

func NewWizardRepo(db *gorm.DB) *WizardRepo { return &WizardRepo{db: db} }

func (r *WizardRepo) Create(ctx context.Context, s *model.WizardSession) error {
	if len(s.StepPayload) == 0 {
		s.StepPayload = []byte("{}")
	}
	if len(s.ProgressLog) == 0 {
		s.ProgressLog = []byte("[]")
	}
	return r.db.WithContext(ctx).Create(s).Error
}

func (r *WizardRepo) Get(ctx context.Context, id string) (*model.WizardSession, error) {
	var s model.WizardSession
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&s).Error
	return &s, err
}

// Update applies the allow-listed field map. Caller owns which keys to
// pass — handler layer decides what the user may patch.
func (r *WizardRepo) Update(ctx context.Context, id string, updates map[string]interface{}) error {
	updates["updated_at"] = time.Now()
	return r.db.WithContext(ctx).Model(&model.WizardSession{}).
		Where("id = ?", id).
		Updates(updates).Error
}

// AppendProgress atomically pushes one entry onto the progress_log
// JSONB array. Use this during Execute so the FE poll loop can read
// partial progress even mid-run.
func (r *WizardRepo) AppendProgress(ctx context.Context, id string, entry map[string]interface{}) error {
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
