// Package main provides helper functions for the benchmark CLI
package main

import (
	"fmt"
	"time"
)

// formatRate formats a rate (items per second)
func formatRate(count int, duration time.Duration) string {
	if duration.Seconds() == 0 {
		return "N/A"
	}
	rate := float64(count) / duration.Seconds()
	return fmt.Sprintf("%.2f/s", rate)
}

// percentageString calculates and formats a percentage
func percentageString(part, total int) string {
	if total == 0 {
		return "0.00%"
	}
	return fmt.Sprintf("%.2f%%", float64(part)/float64(total)*100)
}

// statusEmoji returns an emoji for the workflow status
func statusEmoji(passed, failed, running int) string {
	if running > 0 {
		return "🟡"
	}
	if failed > 0 {
		return "❌"
	}
	if passed > 0 {
		return "✅"
	}
	return "⚪"
}
