package schema

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// IndexingJobStatus represents the status of an address indexing job
type IndexingJobStatus string

const (
	IndexingJobStatusRunning   IndexingJobStatus = "running"
	IndexingJobStatusPaused    IndexingJobStatus = "paused"
	IndexingJobStatusFailed    IndexingJobStatus = "failed"
	IndexingJobStatusCompleted IndexingJobStatus = "completed"
	IndexingJobStatusCanceled  IndexingJobStatus = "canceled"
)

// AddressIndexingJob represents the address_indexing_jobs table
type AddressIndexingJob struct {
	ID              int64             `gorm:"column:id;primaryKey"`
	Address         string            `gorm:"column:address;not null"`
	Chain           domain.Chain      `gorm:"column:chain;not null;type:blockchain_chain"`
	Status          IndexingJobStatus `gorm:"column:status;not null;type:indexing_job_status"`
	WorkflowID      string            `gorm:"column:workflow_id;not null"`
	WorkflowRunID   *string           `gorm:"column:workflow_run_id"`
	TokensProcessed int               `gorm:"column:tokens_processed;default:0"`
	CurrentMinBlock *uint64           `gorm:"column:current_min_block"`
	CurrentMaxBlock *uint64           `gorm:"column:current_max_block"`
	StartedAt       time.Time         `gorm:"column:started_at;not null"`
	PausedAt        *time.Time        `gorm:"column:paused_at"`
	CompletedAt     *time.Time        `gorm:"column:completed_at"`
	FailedAt        *time.Time        `gorm:"column:failed_at"`
	CanceledAt      *time.Time        `gorm:"column:canceled_at"`
	CreatedAt       time.Time         `gorm:"column:created_at;not null;default:now()"`
	UpdatedAt       time.Time         `gorm:"column:updated_at;not null;default:now()"`
}

// TableName specifies the table name for the AddressIndexingJob model
func (AddressIndexingJob) TableName() string {
	return "address_indexing_jobs"
}
