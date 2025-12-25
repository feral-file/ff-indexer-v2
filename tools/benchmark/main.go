package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
)

const (
	defaultTemporalHost = "localhost:7233"
	defaultNamespace    = "ff-indexer"
	pollInterval        = 2 * time.Second // How often to check if workflows are complete
)

type Config struct {
	TemporalHost string
	Namespace    string
	WorkflowID   string
	RunID        string
	Debug        bool
	MaxDepth     int           // Maximum depth to traverse (0 = unlimited)
	MaxWorkflows int           // Maximum number of workflows to collect (0 = unlimited)
	QueryTimeout time.Duration // Timeout for each Temporal query
	OutputFile   string        // Output markdown file path (optional)
	PageSize     int           // Page size for Temporal queries
	Concurrency  int           // Number of concurrent workers
}

type WorkflowStats struct {
	WorkflowID         string
	RunID              string
	WorkflowType       string
	Status             enums.WorkflowExecutionStatus
	StartTime          time.Time
	CloseTime          *time.Time
	ExecutionTime      time.Duration
	Attempt            int32
	ChildWorkflows     map[string]*ChildWorkflowGroup // Grouped by workflow type
	TotalChildren      int
	CompletedChildren  int
	FailedChildren     int
	TerminatedChildren int
	TimedOutChildren   int
	CanceledChildren   int
}

type ChildWorkflowGroup struct {
	WorkflowType    string
	Count           int
	CompletedCount  int
	FailedCount     int
	TerminatedCount int
	TimedOutCount   int
	CanceledCount   int
	FirstStart      time.Time
	LastEnd         *time.Time
	TotalDuration   time.Duration
	Executions      []ChildWorkflowExecution
}

type ChildWorkflowExecution struct {
	WorkflowID    string
	RunID         string
	Status        enums.WorkflowExecutionStatus
	StartTime     time.Time
	CloseTime     *time.Time
	ExecutionTime time.Duration
	Attempt       int32
}

func main() {
	cfg := parseFlags()

	if cfg.WorkflowID == "" {
		fmt.Println("Error: workflow-id is required")
		flag.Usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n\nReceived interrupt signal, shutting down...")
		cancel()
	}()

	// Create Temporal client
	c, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.Namespace,
	})
	if err != nil {
		fmt.Printf("Error creating Temporal client: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	fmt.Printf("Connected to Temporal at %s (namespace: %s)\n", cfg.TemporalHost, cfg.Namespace)
	fmt.Printf("Monitoring workflow: %s\n", cfg.WorkflowID)
	if cfg.RunID != "" {
		fmt.Printf("Run ID: %s\n", cfg.RunID)
	}
	fmt.Printf("\nCollecting workflow statistics...\n")

	// Poll until all workflows are complete
	var lastStats *WorkflowStats
	pollCount := 0

	for {
		select {
		case <-ctx.Done():
			if lastStats != nil {
				fmt.Println("\n\n" + strings.Repeat("=", 80))
				fmt.Println("INTERRUPTED - PARTIAL RESULTS")
				fmt.Println(strings.Repeat("=", 80))
				printWorkflowStats(lastStats)
			}
			return

		default:
			// Non-blocking check for cancellation
		}

		pollCount++
		collectionStart := time.Now()

		stats, err := collectWorkflowStats(ctx, c, cfg)
		if err != nil {
			fmt.Printf("\nError collecting stats: %v\n", err)
			os.Exit(1)
		}

		collectionDuration := time.Since(collectionStart)
		lastStats = stats

		// Show progress indicator (not full stats)
		if !cfg.Debug {
			elapsed := time.Since(stats.StartTime)
			if stats.CloseTime != nil {
				elapsed = stats.CloseTime.Sub(stats.StartTime)
			}

			// Check if all workflows are complete (no running status)
			allComplete := isWorkflowComplete(stats.Status)

			if allComplete {
				fmt.Printf("\r✓ Collection complete (polls: %d, elapsed: %s, collection took: %s)                    \n",
					pollCount, formatDuration(elapsed), formatDuration(collectionDuration))
			} else {
				fmt.Printf("\r⏳ Polling... (polls: %d, elapsed: %s, children: %d total, collection: %s)    ",
					pollCount, formatDuration(elapsed), stats.TotalChildren, formatDuration(collectionDuration))
			}
		}

		// Check if main workflow is complete (we only collect completed workflows)
		if isWorkflowComplete(stats.Status) {
			fmt.Println("\n\n" + strings.Repeat("=", 80))
			fmt.Println("BENCHMARK RESULTS")
			fmt.Println(strings.Repeat("=", 80))
			printWorkflowStats(lastStats)

			// Write to markdown file if specified
			if cfg.OutputFile != "" {
				if err := writeMarkdownReport(cfg.OutputFile, lastStats); err != nil {
					fmt.Printf("\n⚠️  Warning: Failed to write markdown file: %v\n", err)
				} else {
					fmt.Printf("\n✓ Report written to: %s\n", cfg.OutputFile)
				}
			}

			return
		}

		// Wait for poll interval before next collection
		// Use a timer so we can still respond to cancellation
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastStats != nil {
				fmt.Println("\n\n" + strings.Repeat("=", 80))
				fmt.Println("INTERRUPTED - PARTIAL RESULTS")
				fmt.Println(strings.Repeat("=", 80))
				printWorkflowStats(lastStats)
			}
			return
		case <-timer.C:
			// Continue to next poll
		}
	}
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.TemporalHost, "temporal-host", defaultTemporalHost, "Temporal host address")
	flag.StringVar(&cfg.Namespace, "namespace", defaultNamespace, "Temporal namespace")
	flag.StringVar(&cfg.WorkflowID, "workflow-id", "", "Workflow ID to monitor (required)")
	flag.StringVar(&cfg.RunID, "run-id", "", "Specific run ID (optional)")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output markdown file path (optional)")
	flag.BoolVar(&cfg.Debug, "debug", false, "Enable debug logging")
	flag.IntVar(&cfg.MaxDepth, "max-depth", 3, "Maximum depth to traverse (0 = unlimited, default: 3)")
	flag.IntVar(&cfg.MaxWorkflows, "max-workflows", 1000, "Maximum workflows to collect (0 = unlimited, default: 1000)")
	flag.IntVar(&cfg.PageSize, "page-size", 1000, "Page size for Temporal queries (default: 1000, max: 1000)")
	flag.IntVar(&cfg.Concurrency, "concurrency", 5, "Number of concurrent workers (default: 5)")

	var queryTimeoutSeconds int
	flag.IntVar(&queryTimeoutSeconds, "query-timeout", 30, "Timeout for each Temporal query in seconds (default: 30)")

	configFile := flag.String("config", "", "Path to config file (optional)")

	flag.Parse()

	// Convert timeout to duration
	cfg.QueryTimeout = time.Duration(queryTimeoutSeconds) * time.Second

	// Validate page size
	if cfg.PageSize <= 0 || cfg.PageSize > 1000 {
		cfg.PageSize = 1000
	}

	// Validate concurrency
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 5
	}
	if cfg.Concurrency > 10 {
		cfg.Concurrency = 10 // Cap at 10 to avoid overwhelming Temporal
	}

	// Load from config file if specified
	if *configFile != "" {
		fileCfg, err := LoadConfig(*configFile)
		if err != nil {
			fmt.Printf("Warning: failed to load config file: %v\n", err)
		} else {
			// Override with file values if not set via flags
			if cfg.TemporalHost == defaultTemporalHost && fileCfg.TemporalHost != "" {
				cfg.TemporalHost = fileCfg.TemporalHost
			}
			if cfg.Namespace == defaultNamespace && fileCfg.Namespace != "" {
				cfg.Namespace = fileCfg.Namespace
			}
		}
	}

	return cfg
}

// collectWorkflowStats collects comprehensive statistics for a workflow and all its descendants.
// It uses breadth-first traversal to recursively discover child workflows at any depth level.
//
// Example workflow tree:
//
//	Main Workflow (index-token)
//	├── Child 1 (index-metadata)
//	├── Child 2 (index-provenance)
//	│   ├── Grandchild 1 (process-events)
//	│   └── Grandchild 2 (update-balances)
//	└── Child 3 (enhance-metadata)
//
// The function will discover and collect stats for all 6 workflows above.
func collectWorkflowStats(ctx context.Context, c client.Client, cfg *Config) (*WorkflowStats, error) {
	if cfg.Debug {
		fmt.Printf("[DEBUG] Starting collectWorkflowStats for workflow: %s\n", cfg.WorkflowID)
	}

	// Build query for the main workflow
	query := fmt.Sprintf("WorkflowId = '%s'", cfg.WorkflowID)
	if cfg.RunID != "" {
		query = fmt.Sprintf("%s AND RunId = '%s'", query, cfg.RunID)
	}

	if cfg.Debug {
		fmt.Printf("[DEBUG] Query for main workflow: %s\n", query)
	}

	// Get main workflow execution
	queryCtx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout)
	defer cancel()

	resp, err := c.ListWorkflow(queryCtx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: cfg.Namespace,
		Query:     query,
		PageSize:  int32(cfg.PageSize),
	})
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("timeout while querying main workflow (timeout: %v). Try increasing -query-timeout", cfg.QueryTimeout)
		}
		return nil, fmt.Errorf("failed to list workflow: %w", err)
	}

	if len(resp.Executions) == 0 {
		return nil, fmt.Errorf("workflow not found")
	}

	mainExec := resp.Executions[0]
	if cfg.Debug {
		fmt.Printf("[DEBUG] Found main workflow: %s (type: %s, status: %s)\n",
			mainExec.Execution.WorkflowId, mainExec.Type.Name, mainExec.Status)
	}

	stats := &WorkflowStats{
		WorkflowID:     mainExec.Execution.WorkflowId,
		RunID:          mainExec.Execution.RunId,
		WorkflowType:   mainExec.Type.Name,
		Status:         mainExec.Status,
		StartTime:      mainExec.StartTime.AsTime(),
		ChildWorkflows: make(map[string]*ChildWorkflowGroup),
	}

	if mainExec.CloseTime != nil {
		closeTime := mainExec.CloseTime.AsTime()
		stats.CloseTime = &closeTime
		stats.ExecutionTime = closeTime.Sub(stats.StartTime)
	} else {
		stats.ExecutionTime = time.Since(stats.StartTime)
	}

	// Recursively collect all descendant workflows using breadth-first traversal with concurrency
	// This ensures we capture child workflows at any depth level
	var mu sync.Mutex // Protects shared state
	visited := make(map[string]bool)
	depthMap := make(map[string]int)
	workflowsToProcess := []WorkflowIdentifier{
		{WorkflowID: cfg.WorkflowID, RunID: cfg.RunID},
	}
	depthMap[fmt.Sprintf("%s:%s", cfg.WorkflowID, cfg.RunID)] = 0

	if cfg.Debug {
		fmt.Printf("[DEBUG] Starting concurrent collection with %d workers\n", cfg.Concurrency)
		fmt.Printf("[DEBUG] MaxDepth: %d, MaxWorkflows: %d\n", cfg.MaxDepth, cfg.MaxWorkflows)
	}

	totalWorkflowsCollected := 0
	truncated := false
	activeWork := 0 // Track in-flight work items

	// Channel to signal truncation
	truncateChan := make(chan struct{})
	done := make(chan struct{}) // Signal when all work is complete

	// Worker pool pattern
	var wg sync.WaitGroup
	workChan := make(chan WorkflowIdentifier, cfg.Concurrency*2) // Buffered channel

	// Start workers
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for current := range workChan {
				if cfg.Debug {
					fmt.Printf("[DEBUG] Worker %d processing: %s\n", workerID, current.WorkflowID)
				}

				// Get current depth
				mu.Lock()
				key := fmt.Sprintf("%s:%s", current.WorkflowID, current.RunID)
				currentDepth := depthMap[key]
				mu.Unlock()

				// Check if we've reached max depth
				if cfg.MaxDepth > 0 && currentDepth >= cfg.MaxDepth {
					if cfg.Debug {
						fmt.Printf("[DEBUG] Worker %d: Reached max depth (%d) for %s\n", workerID, cfg.MaxDepth, current.WorkflowID)
					}
					// Decrement active work counter
					mu.Lock()
					activeWork--
					if cfg.Debug {
						fmt.Printf("[DEBUG] Worker %d completed (max depth), active work: %d, queue: %d\n",
							workerID, activeWork, len(workflowsToProcess))
					}
					// Check if all work is done
					if activeWork == 0 && len(workflowsToProcess) == 0 {
						select {
						case done <- struct{}{}:
						default:
						}
					}
					mu.Unlock()
					continue
				}

				// Get child workflows
				childQuery := fmt.Sprintf("ParentWorkflowId = '%s'", current.WorkflowID)
				if current.RunID != "" {
					childQuery = fmt.Sprintf("%s AND ParentRunId = '%s'", childQuery, current.RunID)
				}

				if cfg.Debug {
					fmt.Printf("[DEBUG] Worker %d query: %s\n", workerID, childQuery)
				}

				pageToken := []byte(nil)
				pageNum := 0

				for {
					pageNum++

					// Create a new context with timeout for each query
					queryCtx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout)

					childResp, err := c.ListWorkflow(queryCtx, &workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     cfg.Namespace,
						Query:         childQuery,
						NextPageToken: pageToken,
						PageSize:      int32(cfg.PageSize),
					})
					cancel() // Always cancel to free resources

					if err != nil {
						if queryCtx.Err() == context.DeadlineExceeded {
							mu.Lock()
							fmt.Printf("\n⚠️  Warning: Query timeout for workflow %s (page %d). Skipping remaining children.\n", current.WorkflowID, pageNum)
							fmt.Printf("  Try: -query-timeout %d (current: %d seconds)\n\n", int(cfg.QueryTimeout.Seconds())*2, int(cfg.QueryTimeout.Seconds()))
							mu.Unlock()
							break
						}
						mu.Lock()
						fmt.Printf("\n⚠️  Error querying workflow %s: %v\n", current.WorkflowID, err)
						mu.Unlock()
						break
					}

					if cfg.Debug {
						fmt.Printf("[DEBUG] Worker %d found %d children in page %d\n", workerID, len(childResp.Executions), pageNum)
					}

					// Process children and add to queue
					shouldStop := false
					mu.Lock()
					for _, childExec := range childResp.Executions {
						// Process this child workflow
						processChildWorkflow(stats, childExec)

						// Add to queue for recursive processing
						childKey := fmt.Sprintf("%s:%s", childExec.Execution.WorkflowId, childExec.Execution.RunId)

						// Skip if already visited or queued
						if visited[childKey] {
							if cfg.Debug {
								fmt.Printf("[DEBUG] Worker %d skipping already visited: %s\n", workerID, childKey)
							}
							continue
						}

						// Mark as visited to prevent adding to queue multiple times
						visited[childKey] = true
						totalWorkflowsCollected++

						// Check if we've hit the workflow limit
						if cfg.MaxWorkflows > 0 && totalWorkflowsCollected >= cfg.MaxWorkflows {
							truncated = true
							shouldStop = true
							select {
							case truncateChan <- struct{}{}:
							default:
							}
							break
						}

						depthMap[childKey] = currentDepth + 1
						workflowsToProcess = append(workflowsToProcess, WorkflowIdentifier{
							WorkflowID: childExec.Execution.WorkflowId,
							RunID:      childExec.Execution.RunId,
						})

						if cfg.Debug {
							fmt.Printf("[DEBUG] Worker %d added to queue at depth %d: %s (queue size: %d)\n",
								workerID, currentDepth+1, childExec.Execution.WorkflowId, len(workflowsToProcess))
						}
					}
					mu.Unlock()

					if shouldStop {
						return
					}

					pageToken = childResp.NextPageToken
					if len(pageToken) == 0 {
						break
					}
				}

				// Decrement active work counter after processing this workflow
				mu.Lock()
				activeWork--
				if cfg.Debug {
					fmt.Printf("[DEBUG] Worker %d completed %s, active work: %d, queue: %d\n",
						workerID, current.WorkflowID, activeWork, len(workflowsToProcess))
				}
				// Check if all work is done
				if activeWork == 0 && len(workflowsToProcess) == 0 {
					select {
					case done <- struct{}{}:
					default:
					}
				}
				mu.Unlock()
			}
		}(i)
	}

	// Feed work to workers
	go func() {
		defer close(workChan)

		idleChecks := 0         // Counter for consecutive idle checks
		const maxIdleChecks = 5 // Wait for 5 consecutive idle checks before exiting

		for {
			// Check for truncation or cancellation
			select {
			case <-truncateChan:
				return
			case <-ctx.Done():
				return
			case <-done:
				if cfg.Debug {
					fmt.Printf("[DEBUG] Feeder received done signal\n")
				}
				return
			default:
			}

			mu.Lock()
			queueLen := len(workflowsToProcess)
			active := activeWork

			// Check if we have work to do
			if queueLen == 0 {
				// No work in queue - check if workers are still busy
				if active == 0 {
					// Queue empty and no active work
					idleChecks++
					if cfg.Debug {
						fmt.Printf("[DEBUG] Feeder: idle check %d/%d (queue: %d, active: %d)\n",
							idleChecks, maxIdleChecks, queueLen, active)
					}

					if idleChecks >= maxIdleChecks {
						// Been idle for multiple checks, we're really done
						if cfg.Debug {
							fmt.Printf("[DEBUG] Feeder: exiting after %d idle checks\n", idleChecks)
						}
						mu.Unlock()
						return
					}

					// Wait a bit and check again
					mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					continue
				}
				// Workers are still busy, wait a bit
				if cfg.Debug {
					fmt.Printf("[DEBUG] Feeder: waiting for workers (queue: %d, active: %d)\n", queueLen, active)
				}
				idleChecks = 0 // Reset idle counter
				mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				continue
			}

			// We have work - reset idle counter
			idleChecks = 0

			// Pop the first workflow
			current := workflowsToProcess[0]
			workflowsToProcess = workflowsToProcess[1:]

			// Note: visited check is done by workers when adding to queue,
			// so we don't need to check again here

			// Increment active work before sending
			activeWork++
			queueSize := len(workflowsToProcess)

			if cfg.Debug {
				fmt.Printf("[DEBUG] Feeder sending %s to workers (active: %d, queue: %d)\n",
					current.WorkflowID, activeWork, queueSize)
			}

			mu.Unlock()

			// Show progress
			if !cfg.Debug && totalWorkflowsCollected%50 == 0 {
				fmt.Printf("  Collecting workflows... (visited: %d, queue: %d)\n",
					totalWorkflowsCollected, queueSize)
			}

			// Send to worker
			select {
			case workChan <- current:
			case <-truncateChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all workers to finish
	wg.Wait()

	if cfg.Debug {
		fmt.Printf("[DEBUG] Concurrent collection complete\n")
		fmt.Printf("[DEBUG] Total workflows visited: %d\n", totalWorkflowsCollected)
		fmt.Printf("[DEBUG] Total child workflow groups: %d\n", len(stats.ChildWorkflows))
		if truncated {
			fmt.Printf("[DEBUG] Collection was truncated (reached limits)\n")
		}
	}

	if truncated {
		fmt.Printf("\n⚠️  Warning: Collection was truncated. Increase -max-depth or -max-workflows for complete results.\n")
		fmt.Printf("  Collected: %d workflows, Max depth reached: %d\n\n", totalWorkflowsCollected, cfg.MaxDepth)
	}

	// Calculate aggregated stats for each child workflow group
	for _, group := range stats.ChildWorkflows {
		calculateGroupStats(group)
	}

	return stats, nil
}

// WorkflowIdentifier uniquely identifies a workflow execution
type WorkflowIdentifier struct {
	WorkflowID string
	RunID      string
}

func processChildWorkflow(stats *WorkflowStats, exec *workflowpb.WorkflowExecutionInfo) {
	workflowType := exec.Type.Name
	startTime := exec.StartTime.AsTime()

	var closeTime *time.Time
	var executionTime time.Duration

	if exec.CloseTime != nil {
		ct := exec.CloseTime.AsTime()
		closeTime = &ct
		executionTime = ct.Sub(startTime)
	} else {
		executionTime = time.Since(startTime)
	}

	// Get or create workflow group
	group, exists := stats.ChildWorkflows[workflowType]
	if !exists {
		group = &ChildWorkflowGroup{
			WorkflowType: workflowType,
			FirstStart:   startTime,
			Executions:   []ChildWorkflowExecution{},
		}
		stats.ChildWorkflows[workflowType] = group
	}

	// Update group stats
	group.Count++
	stats.TotalChildren++

	// Track first start and last end
	if startTime.Before(group.FirstStart) {
		group.FirstStart = startTime
	}
	if closeTime != nil {
		if group.LastEnd == nil || closeTime.After(*group.LastEnd) {
			group.LastEnd = closeTime
		}
	}

	// Count by status (track each status separately)
	switch exec.Status {
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		group.CompletedCount++
		stats.CompletedChildren++
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		group.FailedCount++
		stats.FailedChildren++
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		group.TerminatedCount++
		stats.TerminatedChildren++
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		group.TimedOutCount++
		stats.TimedOutChildren++
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		group.CanceledCount++
		stats.CanceledChildren++
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		// Skip running workflows - we only report after everything is done
		return
	}

	// Add execution details
	execution := ChildWorkflowExecution{
		WorkflowID:    exec.Execution.WorkflowId,
		RunID:         exec.Execution.RunId,
		Status:        exec.Status,
		StartTime:     startTime,
		CloseTime:     closeTime,
		ExecutionTime: executionTime,
		Attempt:       1, // Temporal doesn't expose attempt directly in list API
	}
	group.Executions = append(group.Executions, execution)
}

func calculateGroupStats(group *ChildWorkflowGroup) {
	if group.LastEnd != nil {
		group.TotalDuration = group.LastEnd.Sub(group.FirstStart)
	} else {
		group.TotalDuration = time.Since(group.FirstStart)
	}
}

func printWorkflowStats(stats *WorkflowStats) {
	// Main workflow info
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Main Workflow: %s\n", stats.WorkflowType)
	fmt.Printf("  Workflow ID: %s\n", stats.WorkflowID)
	fmt.Printf("  Run ID:      %s\n", stats.RunID)
	fmt.Printf("  Status:      %s\n", formatStatus(stats.Status))
	fmt.Printf("  Start Time:  %s\n", stats.StartTime.Format("2006-01-02 15:04:05"))
	if stats.CloseTime != nil {
		fmt.Printf("  Close Time:  %s\n", stats.CloseTime.Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("  Duration:    %s\n", formatDuration(stats.ExecutionTime))
	fmt.Println()

	// Summary
	fmt.Printf("Child Workflows Summary:\n")
	fmt.Printf("  Total:       %d\n", stats.TotalChildren)
	fmt.Printf("  Completed:   %d\n", stats.CompletedChildren)
	if stats.FailedChildren > 0 {
		fmt.Printf("  Failed:      %d\n", stats.FailedChildren)
	}
	if stats.TerminatedChildren > 0 {
		fmt.Printf("  Terminated:  %d\n", stats.TerminatedChildren)
	}
	if stats.TimedOutChildren > 0 {
		fmt.Printf("  Timed Out:   %d\n", stats.TimedOutChildren)
	}
	if stats.CanceledChildren > 0 {
		fmt.Printf("  Canceled:    %d\n", stats.CanceledChildren)
	}
	fmt.Println()

	if len(stats.ChildWorkflows) == 0 {
		fmt.Println("No child workflows found.")
		fmt.Println(strings.Repeat("-", 80))
		return
	}

	// Sort groups by workflow type name
	var sortedGroups []*ChildWorkflowGroup
	for _, group := range stats.ChildWorkflows {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].WorkflowType < sortedGroups[j].WorkflowType
	})

	// Detailed breakdown by workflow type
	fmt.Println("Child Workflows Breakdown:")
	fmt.Println()

	for _, group := range sortedGroups {
		totalFailures := group.FailedCount + group.TerminatedCount + group.TimedOutCount + group.CanceledCount
		emoji := statusEmoji(group.CompletedCount, totalFailures, 0)
		fmt.Printf("  %s %s\n", emoji, group.WorkflowType)
		fmt.Printf("    Count:          %d\n", group.Count)
		fmt.Printf("    Completed:      %d (%s)\n", group.CompletedCount, percentageString(group.CompletedCount, group.Count))
		if group.FailedCount > 0 {
			fmt.Printf("    Failed:         %d (%s)\n", group.FailedCount, percentageString(group.FailedCount, group.Count))
		}
		if group.TerminatedCount > 0 {
			fmt.Printf("    Terminated:     %d (%s)\n", group.TerminatedCount, percentageString(group.TerminatedCount, group.Count))
		}
		if group.TimedOutCount > 0 {
			fmt.Printf("    Timed Out:      %d (%s)\n", group.TimedOutCount, percentageString(group.TimedOutCount, group.Count))
		}
		if group.CanceledCount > 0 {
			fmt.Printf("    Canceled:       %d (%s)\n", group.CanceledCount, percentageString(group.CanceledCount, group.Count))
		}
		fmt.Printf("    First Start:    %s\n", group.FirstStart.Format("15:04:05"))
		if group.LastEnd != nil {
			fmt.Printf("    Last End:       %s\n", group.LastEnd.Format("15:04:05"))
		}
		fmt.Printf("    Total Duration: %s\n", formatDuration(group.TotalDuration))
		if group.Count > 0 && group.TotalDuration > 0 {
			fmt.Printf("    Avg Rate:       %s\n", formatRate(group.Count, group.TotalDuration))
		}
		fmt.Println()
	}

	fmt.Println(strings.Repeat("-", 80))
}

func formatStatus(status enums.WorkflowExecutionStatus) string {
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "🟡 RUNNING"
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "✅ COMPLETED"
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "❌ FAILED"
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "🚫 CANCELED"
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "⛔ TERMINATED"
	case enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "🔄 CONTINUED_AS_NEW"
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "⏱️ TIMED_OUT"
	default:
		return status.String()
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	return fmt.Sprintf("%dh %dm", hours, minutes)
}

func isWorkflowComplete(status enums.WorkflowExecutionStatus) bool {
	return status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// writeMarkdownReport writes a markdown report of the workflow stats
func writeMarkdownReport(filepath string, stats *WorkflowStats) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer func() {
		_ = file.Close()
	}()

	// Write header
	_, _ = fmt.Fprintf(file, "# Workflow Benchmark Report\n\n")
	_, _ = fmt.Fprintf(file, "Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05"))

	// Main workflow section
	_, _ = fmt.Fprintf(file, "## Main Workflow\n\n")
	_, _ = fmt.Fprintf(file, "| Property | Value |\n")
	_, _ = fmt.Fprintf(file, "|----------|-------|\n")
	_, _ = fmt.Fprintf(file, "| **Workflow Type** | %s |\n", stats.WorkflowType)
	_, _ = fmt.Fprintf(file, "| **Workflow ID** | `%s` |\n", stats.WorkflowID)
	_, _ = fmt.Fprintf(file, "| **Run ID** | `%s` |\n", stats.RunID)
	_, _ = fmt.Fprintf(file, "| **Status** | %s |\n", formatStatusMarkdown(stats.Status))
	_, _ = fmt.Fprintf(file, "| **Start Time** | %s |\n", stats.StartTime.Format("2006-01-02 15:04:05"))
	if stats.CloseTime != nil {
		_, _ = fmt.Fprintf(file, "| **Close Time** | %s |\n", stats.CloseTime.Format("2006-01-02 15:04:05"))
	}
	_, _ = fmt.Fprintf(file, "| **Duration** | %s |\n", formatDuration(stats.ExecutionTime))
	_, _ = fmt.Fprintf(file, "\n")

	// Summary section
	_, _ = fmt.Fprintf(file, "## Child Workflows Summary\n\n")
	_, _ = fmt.Fprintf(file, "| Metric | Count |\n")
	_, _ = fmt.Fprintf(file, "|--------|-------|\n")
	_, _ = fmt.Fprintf(file, "| **Total** | %d |\n", stats.TotalChildren)
	_, _ = fmt.Fprintf(file, "| **Completed** | %d |\n", stats.CompletedChildren)
	if stats.FailedChildren > 0 {
		_, _ = fmt.Fprintf(file, "| **Failed** | %d |\n", stats.FailedChildren)
	}
	if stats.TerminatedChildren > 0 {
		_, _ = fmt.Fprintf(file, "| **Terminated** | %d |\n", stats.TerminatedChildren)
	}
	if stats.TimedOutChildren > 0 {
		_, _ = fmt.Fprintf(file, "| **Timed Out** | %d |\n", stats.TimedOutChildren)
	}
	if stats.CanceledChildren > 0 {
		_, _ = fmt.Fprintf(file, "| **Canceled** | %d |\n", stats.CanceledChildren)
	}
	_, _ = fmt.Fprintf(file, "\n")

	if len(stats.ChildWorkflows) == 0 {
		_, _ = fmt.Fprintf(file, "*No child workflows found.*\n")
		return nil
	}

	// Sort groups by workflow type name
	var sortedGroups []*ChildWorkflowGroup
	for _, group := range stats.ChildWorkflows {
		sortedGroups = append(sortedGroups, group)
	}
	sort.Slice(sortedGroups, func(i, j int) bool {
		return sortedGroups[i].WorkflowType < sortedGroups[j].WorkflowType
	})

	// Detailed breakdown
	_, _ = fmt.Fprintf(file, "## Child Workflows Breakdown\n\n")

	for _, group := range sortedGroups {
		totalFailures := group.FailedCount + group.TerminatedCount + group.TimedOutCount + group.CanceledCount
		emoji := statusEmoji(group.CompletedCount, totalFailures, 0)
		_, _ = fmt.Fprintf(file, "### %s %s\n\n", emoji, group.WorkflowType)

		_, _ = fmt.Fprintf(file, "| Metric | Value |\n")
		_, _ = fmt.Fprintf(file, "|--------|-------|\n")
		_, _ = fmt.Fprintf(file, "| **Count** | %d |\n", group.Count)
		_, _ = fmt.Fprintf(file, "| **Completed** | %d (%s) |\n", group.CompletedCount, percentageString(group.CompletedCount, group.Count))
		if group.FailedCount > 0 {
			_, _ = fmt.Fprintf(file, "| **Failed** | %d (%s) |\n", group.FailedCount, percentageString(group.FailedCount, group.Count))
		}
		if group.TerminatedCount > 0 {
			_, _ = fmt.Fprintf(file, "| **Terminated** | %d (%s) |\n", group.TerminatedCount, percentageString(group.TerminatedCount, group.Count))
		}
		if group.TimedOutCount > 0 {
			_, _ = fmt.Fprintf(file, "| **Timed Out** | %d (%s) |\n", group.TimedOutCount, percentageString(group.TimedOutCount, group.Count))
		}
		if group.CanceledCount > 0 {
			_, _ = fmt.Fprintf(file, "| **Canceled** | %d (%s) |\n", group.CanceledCount, percentageString(group.CanceledCount, group.Count))
		}
		_, _ = fmt.Fprintf(file, "| **First Start** | %s |\n", group.FirstStart.Format("15:04:05"))
		if group.LastEnd != nil {
			_, _ = fmt.Fprintf(file, "| **Last End** | %s |\n", group.LastEnd.Format("15:04:05"))
		}
		_, _ = fmt.Fprintf(file, "| **Total Duration** | %s |\n", formatDuration(group.TotalDuration))
		if group.Count > 0 && group.TotalDuration > 0 {
			_, _ = fmt.Fprintf(file, "| **Avg Rate** | %s |\n", formatRate(group.Count, group.TotalDuration))
		}
		_, _ = fmt.Fprintf(file, "\n")
	}

	// Add metadata section for LLM
	_, _ = fmt.Fprintf(file, "---\n\n")
	_, _ = fmt.Fprintf(file, "## Metadata (for LLM processing)\n\n")
	_, _ = fmt.Fprintf(file, "```json\n")
	_, _ = fmt.Fprintf(file, "{\n")
	_, _ = fmt.Fprintf(file, "  \"workflow_id\": \"%s\",\n", stats.WorkflowID)
	_, _ = fmt.Fprintf(file, "  \"workflow_type\": \"%s\",\n", stats.WorkflowType)
	_, _ = fmt.Fprintf(file, "  \"status\": \"%s\",\n", stats.Status.String())
	_, _ = fmt.Fprintf(file, "  \"duration_seconds\": %.2f,\n", stats.ExecutionTime.Seconds())
	_, _ = fmt.Fprintf(file, "  \"total_children\": %d,\n", stats.TotalChildren)
	_, _ = fmt.Fprintf(file, "  \"completed_children\": %d,\n", stats.CompletedChildren)
	_, _ = fmt.Fprintf(file, "  \"failed_children\": %d,\n", stats.FailedChildren)
	_, _ = fmt.Fprintf(file, "  \"terminated_children\": %d,\n", stats.TerminatedChildren)
	_, _ = fmt.Fprintf(file, "  \"timed_out_children\": %d,\n", stats.TimedOutChildren)
	_, _ = fmt.Fprintf(file, "  \"canceled_children\": %d,\n", stats.CanceledChildren)
	_, _ = fmt.Fprintf(file, "  \"child_workflow_types\": [\n")
	for i, group := range sortedGroups {
		_, _ = fmt.Fprintf(file, "    {\n")
		_, _ = fmt.Fprintf(file, "      \"type\": \"%s\",\n", group.WorkflowType)
		_, _ = fmt.Fprintf(file, "      \"count\": %d,\n", group.Count)
		_, _ = fmt.Fprintf(file, "      \"completed\": %d,\n", group.CompletedCount)
		_, _ = fmt.Fprintf(file, "      \"failed\": %d,\n", group.FailedCount)
		_, _ = fmt.Fprintf(file, "      \"terminated\": %d,\n", group.TerminatedCount)
		_, _ = fmt.Fprintf(file, "      \"timed_out\": %d,\n", group.TimedOutCount)
		_, _ = fmt.Fprintf(file, "      \"canceled\": %d,\n", group.CanceledCount)
		_, _ = fmt.Fprintf(file, "      \"duration_seconds\": %.2f\n", group.TotalDuration.Seconds())
		if i < len(sortedGroups)-1 {
			_, _ = fmt.Fprintf(file, "    },\n")
		} else {
			_, _ = fmt.Fprintf(file, "    }\n")
		}
	}
	_, _ = fmt.Fprintf(file, "  ]\n")
	_, _ = fmt.Fprintf(file, "}\n")
	_, _ = fmt.Fprintf(file, "```\n")

	return nil
}

func formatStatusMarkdown(status enums.WorkflowExecutionStatus) string {
	switch status {
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return "🟡 RUNNING"
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return "✅ COMPLETED"
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED:
		return "❌ FAILED"
	case enums.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return "🚫 CANCELED"
	case enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return "⛔ TERMINATED"
	case enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return "🔄 CONTINUED_AS_NEW"
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return "⏱️ TIMED_OUT"
	default:
		return status.String()
	}
}
