package schema

import (
	"time"

	"gorm.io/datatypes"
)

// JobStatus is the status column for rows in the jobs table (postgres job queue).
type JobStatus string

const (
	// JobStatusPending is waiting to be claimed by a worker.
	JobStatusPending JobStatus = "pending"
	// JobStatusRunning is currently being processed by a worker.
	JobStatusRunning JobStatus = "running"
	// JobStatusSucceeded means the handler completed without error.
	JobStatusSucceeded JobStatus = "succeeded"
	// JobStatusFailed means the handler returned a terminal error (v1: no auto-retry).
	JobStatusFailed JobStatus = "failed"
	// JobStatusCanceled means the job was stopped after cancel was requested and observed.
	JobStatusCanceled JobStatus = "canceled"
)

// Job maps to the jobs table. Persistence and queries live in the store package; this type is the GORM model only.
type Job struct {
	// ID is the primary key.
	ID int64 `gorm:"column:id;primaryKey;autoIncrement"`
	// Queue is the logical queue name (e.g. token_index, media_index).
	Queue string `gorm:"column:queue;not null"`
	// Kind is the registered handler / workflow name.
	Kind string `gorm:"column:kind;not null"`
	// Payload holds handler arguments (JSON array of raw values on the wire).
	Payload datatypes.JSON `gorm:"column:payload;not null;type:jsonb"`
	// Status is the job lifecycle state.
	Status JobStatus `gorm:"column:status;not null;type:job_status;default:pending"`
	// UniqueKey, when set, participates in partial unique index for active jobs (pending or running) per (queue, kind, key).
	UniqueKey *string `gorm:"column:unique_key"`
	// RunAfter is the earliest time the job may be claimed.
	RunAfter time.Time `gorm:"column:run_after;not null;default:now()"`
	// LastError stores the failure message for failed jobs.
	LastError *string `gorm:"column:last_error"`
	// CancelRequested is set by RequestJobCancel; the worker stops the handler and moves to canceled when observed.
	CancelRequested bool `gorm:"column:cancel_requested;not null;default:false"`
	// CreatedAt is when the row was inserted.
	CreatedAt time.Time `gorm:"column:created_at;not null;default:now()"`
	// UpdatedAt is maintained by the updated_at trigger.
	UpdatedAt time.Time `gorm:"column:updated_at;not null;default:now()"`
	// StartedAt is set when the job is claimed (status becomes running).
	StartedAt *time.Time `gorm:"column:started_at"`
	// FinishedAt is set when the job reaches a terminal state (succeeded, failed, or canceled).
	FinishedAt *time.Time `gorm:"column:finished_at"`
}

// TableName returns the GORM table name for Job.
func (Job) TableName() string {
	return "jobs"
}
