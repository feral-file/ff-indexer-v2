package main

import (
	"testing"
	"time"

	"go.temporal.io/api/enums/v1"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "milliseconds",
			duration: 500 * time.Millisecond,
			want:     "500ms",
		},
		{
			name:     "seconds",
			duration: 5 * time.Second,
			want:     "5.00s",
		},
		{
			name:     "minutes",
			duration: 2*time.Minute + 30*time.Second,
			want:     "2m 30s",
		},
		{
			name:     "hours",
			duration: 1*time.Hour + 15*time.Minute,
			want:     "1h 15m",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatDuration(tt.duration)
			if got != tt.want {
				t.Errorf("formatDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatStatus(t *testing.T) {
	tests := []struct {
		name   string
		status enums.WorkflowExecutionStatus
		want   string
	}{
		{
			name:   "running",
			status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			want:   "🟡 RUNNING",
		},
		{
			name:   "completed",
			status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			want:   "✅ COMPLETED",
		},
		{
			name:   "failed",
			status: enums.WORKFLOW_EXECUTION_STATUS_FAILED,
			want:   "❌ FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatStatus(tt.status)
			if got != tt.want {
				t.Errorf("formatStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsWorkflowComplete(t *testing.T) {
	tests := []struct {
		name   string
		status enums.WorkflowExecutionStatus
		want   bool
	}{
		{
			name:   "running is not complete",
			status: enums.WORKFLOW_EXECUTION_STATUS_RUNNING,
			want:   false,
		},
		{
			name:   "completed is complete",
			status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			want:   true,
		},
		{
			name:   "failed is complete",
			status: enums.WORKFLOW_EXECUTION_STATUS_FAILED,
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isWorkflowComplete(tt.status)
			if got != tt.want {
				t.Errorf("isWorkflowComplete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPercentageString(t *testing.T) {
	tests := []struct {
		name  string
		part  int
		total int
		want  string
	}{
		{
			name:  "50 percent",
			part:  1,
			total: 2,
			want:  "50.00%",
		},
		{
			name:  "100 percent",
			part:  5,
			total: 5,
			want:  "100.00%",
		},
		{
			name:  "0 percent",
			part:  0,
			total: 5,
			want:  "0.00%",
		},
		{
			name:  "division by zero",
			part:  5,
			total: 0,
			want:  "0.00%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentageString(tt.part, tt.total)
			if got != tt.want {
				t.Errorf("percentageString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStatusEmoji(t *testing.T) {
	tests := []struct {
		name    string
		passed  int
		failed  int
		running int
		want    string
	}{
		{
			name:    "running",
			passed:  0,
			failed:  0,
			running: 1,
			want:    "🟡",
		},
		{
			name:    "failed",
			passed:  1,
			failed:  1,
			running: 0,
			want:    "❌",
		},
		{
			name:    "passed",
			passed:  5,
			failed:  0,
			running: 0,
			want:    "✅",
		},
		{
			name:    "none",
			passed:  0,
			failed:  0,
			running: 0,
			want:    "⚪",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := statusEmoji(tt.passed, tt.failed, tt.running)
			if got != tt.want {
				t.Errorf("statusEmoji() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFormatRate(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		duration time.Duration
		want     string
	}{
		{
			name:     "1 per second",
			count:    10,
			duration: 10 * time.Second,
			want:     "1.00/s",
		},
		{
			name:     "2 per second",
			count:    20,
			duration: 10 * time.Second,
			want:     "2.00/s",
		},
		{
			name:     "zero duration",
			count:    10,
			duration: 0,
			want:     "N/A",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatRate(tt.count, tt.duration)
			if got != tt.want {
				t.Errorf("formatRate() = %v, want %v", got, tt.want)
			}
		})
	}
}
