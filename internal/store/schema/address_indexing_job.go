package schema

import (
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
)

// IndexingJobStatus represents the status of an address indexing job
type IndexingJobStatus string

const (
	// IndexingJobStatusRunning is the status of a running indexing job
	IndexingJobStatusRunning IndexingJobStatus = "running"
	// IndexingJobStatusPaused is the status of a paused indexing job
	IndexingJobStatusPaused IndexingJobStatus = "paused"
	// IndexingJobStatusFailed is the status of a failed indexing job
	IndexingJobStatusFailed IndexingJobStatus = "failed"
	// IndexingJobStatusCompleted is the status of a completed indexing job
	IndexingJobStatusCompleted IndexingJobStatus = "completed"
	// IndexingJobStatusCanceled is the status of a canceled indexing job
	IndexingJobStatusCanceled IndexingJobStatus = "canceled"
)

// AddressIndexingJob represents the address_indexing_jobs table
type AddressIndexingJob struct {
	// ID is the internal database primary key
	ID int64 `gorm:"column:id;primaryKey"`
	// Address is the blockchain address being indexed
	Address string `gorm:"column:address;not null"`
	// Chain is the blockchain network being indexed
	Chain domain.Chain `gorm:"column:chain;not null;type:blockchain_chain"`
	// Status is the status of the indexing job
	Status IndexingJobStatus `gorm:"column:status;not null;type:indexing_job_status"`
	// WorkflowID is the orchestrator workflow ID handling this job
	WorkflowID string `gorm:"column:workflow_id;not null"`
	// WorkflowRunID is the orchestrator workflow run ID for this workflow execution
	WorkflowRunID *string `gorm:"column:workflow_run_id"`
	// TokensProcessed is the number of tokens processed by this job
	TokensProcessed int `gorm:"column:tokens_processed;default:0"`
	// CurrentMinBlock is the current minimum block being indexed
	CurrentMinBlock *uint64 `gorm:"column:current_min_block"`
	// CurrentMaxBlock is the current maximum block being indexed
	CurrentMaxBlock *uint64 `gorm:"column:current_max_block"`
	// StartedAt is the timestamp when the job started
	StartedAt time.Time `gorm:"column:started_at;not null"`
	// PausedAt is the timestamp when the job was paused
	PausedAt *time.Time `gorm:"column:paused_at"`
	// CompletedAt is the timestamp when the job completed successfully
	CompletedAt *time.Time `gorm:"column:completed_at"`
	// FailedAt is the timestamp when the job failed
	FailedAt *time.Time `gorm:"column:failed_at"`
	// CanceledAt is the timestamp when the job was canceled
	CanceledAt *time.Time `gorm:"column:canceled_at"`
	// CreatedAt is the timestamp when the job was created
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now()"`
	// UpdatedAt is the timestamp when the job was last updated
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now()"`
}

// TableName specifies the table name for the AddressIndexingJob model
func (AddressIndexingJob) TableName() string {
	return "address_indexing_jobs"
}
