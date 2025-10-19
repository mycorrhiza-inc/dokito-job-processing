package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
)

type DokitoBinaryPaths struct {
	ProcessDocketPath      string
	UploadDocketPath       string
	DownloadAttachmentPath string
}

func getDokitoPaths() DokitoBinaryPaths {
	// Get binary paths from environment variables - required
	processDocketsBinaryPath := os.Getenv("DOKITO_PROCESS_DOCKETS_BINARY_PATH")
	uploadDocketsBinaryPath := os.Getenv("DOKITO_UPLOAD_DOCKETS_BINARY_PATH")
	downloadAttachmentsBinaryPath := os.Getenv("DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH")

	if processDocketsBinaryPath == "" {
		log.Fatal("❌ DOKITO_PROCESS_DOCKETS_BINARY_PATH environment variable is required")
	}
	if uploadDocketsBinaryPath == "" {
		log.Fatal("❌ DOKITO_UPLOAD_DOCKETS_BINARY_PATH environment variable is required")
	}
	if downloadAttachmentsBinaryPath == "" {
		log.Fatal("❌ DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH environment variable is required")
	}
	return DokitoBinaryPaths{
		ProcessDocketPath:      processDocketsBinaryPath,
		UploadDocketPath:       uploadDocketsBinaryPath,
		DownloadAttachmentPath: downloadAttachmentsBinaryPath,
	}
}

// JobType represents the category of job
type JobType string

const (
	JobTypeScrape   JobType = "scrape"
	JobTypeDokito   JobType = "dokito"
	JobTypePipeline JobType = "pipeline"
	JobTypeCaseList JobType = "caselist"
)

// ScrapingMode represents different scraping modes from the API
type ScrapingMode string

const (
	// NY PUC scraping modes
	ScrapingModeAll                 ScrapingMode = "all"
	ScrapingModeFull                ScrapingMode = "full"
	ScrapingModeMetadata            ScrapingMode = "meta"
	ScrapingModeDocuments           ScrapingMode = "docs"
	ScrapingModeParties             ScrapingMode = "parties"
	ScrapingModeDates               ScrapingMode = "dates"
	ScrapingModeFullExtraction      ScrapingMode = "full-extraction"
	ScrapingModeFilingsBetweenDates ScrapingMode = "filings-between-dates"
	ScrapingModeCaseList            ScrapingMode = "case-list"
)

// PipelineStage represents stages in the processing pipeline
type PipelineStage string

const (
	PipelineStageScrape  PipelineStage = "scrape"
	PipelineStageUpload  PipelineStage = "upload"
	PipelineStageProcess PipelineStage = "process"
	PipelineStageIngest  PipelineStage = "ingest"
)

// DokitoMode represents different Dokito backend operations
type DokitoMode string

const (
	DokitoModeCaseFetch     DokitoMode = "case-fetch"
	DokitoModeCaseList      DokitoMode = "caselist"
	DokitoModeAttachmentObj DokitoMode = "attachment-obj"
	DokitoModeAttachmentRaw DokitoMode = "attachment-raw"
	DokitoModeCaseSubmit    DokitoMode = "case-submit"
	DokitoModeReprocess     DokitoMode = "reprocess"
)

// BaseJob represents common job fields
type BaseJob struct {
	ID     string   `json:"id"`
	Type   JobType  `json:"type"`
	GovIDs []string `json:"gov_ids"`
}

// ScrapeJob represents a scraping job
type ScrapeJob struct {
	BaseJob
	Mode       ScrapingMode `json:"mode"`
	DateString string       `json:"date_string,omitempty"`
	BeginDate  string       `json:"begin_date,omitempty"`
	EndDate    string       `json:"end_date,omitempty"`
}

// DokitoJob represents a Dokito backend job
type DokitoJob struct {
	BaseJob
	Mode             DokitoMode  `json:"mode"`
	State            string      `json:"state,omitempty"`
	JurisdictionName string      `json:"jurisdiction_name,omitempty"`
	CaseName         string      `json:"case_name,omitempty"`
	Blake2bHash      string      `json:"blake2b_hash,omitempty"`
	CaseData         interface{} `json:"case_data,omitempty"`
	OperationType    string      `json:"operation_type,omitempty"`
	Limit            int         `json:"limit,omitempty"`
	Offset           int         `json:"offset,omitempty"`
}

// CaseListJob represents a job to fetch the complete caselist
type CaseListJob struct {
	BaseJob
}

// TimeoutConfig represents timeout configuration for jobs
type TimeoutConfig struct {
	ScrapeTimeoutMinutes    int `json:"scrape_timeout_minutes,omitempty"`
	UploadTimeoutMinutes    int `json:"upload_timeout_minutes,omitempty"`
	ProcessTimeoutMinutes   int `json:"process_timeout_minutes,omitempty"`
	IngestTimeoutMinutes    int `json:"ingest_timeout_minutes,omitempty"`
	ContainerTimeoutMinutes int `json:"container_timeout_minutes,omitempty"`
	HTTPTimeoutSeconds      int `json:"http_timeout_seconds,omitempty"`
}

// RetryConfig represents retry configuration for pipeline stages
type RetryConfig struct {
	MaxRetries      int           `json:"max_retries,omitempty"`
	InitialDelay    time.Duration `json:"initial_delay,omitempty"`
	MaxDelay        time.Duration `json:"max_delay,omitempty"`
	BackoffFactor   float64       `json:"backoff_factor,omitempty"`
	RetryableErrors []string      `json:"retryable_errors,omitempty"`
}

// GetDefaultRetryConfig returns default retry configuration
func GetDefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:    3,
		InitialDelay:  5 * time.Second,
		MaxDelay:      60 * time.Second,
		BackoffFactor: 2.0,
		RetryableErrors: []string{
			"connection refused",
			"timeout",
			"temporary failure",
			"service unavailable",
			"internal server error",
		},
	}
}

// GetDefaultTimeouts returns default timeout configuration
func GetDefaultTimeouts() TimeoutConfig {
	return TimeoutConfig{
		ScrapeTimeoutMinutes:    10,
		UploadTimeoutMinutes:    5,
		ProcessTimeoutMinutes:   15,
		IngestTimeoutMinutes:    10,
		ContainerTimeoutMinutes: 10,
		HTTPTimeoutSeconds:      30,
	}
}

// PipelineJob represents a multi-stage pipeline job
type PipelineJob struct {
	BaseJob
	Stages       []PipelineStage               `json:"stages"`
	CurrentStage PipelineStage                 `json:"current_stage"`
	StageConfig  map[string]interface{}        `json:"stage_config,omitempty"`
	ParentJobID  string                        `json:"parent_job_id,omitempty"`
	Results      map[PipelineStage]interface{} `json:"results,omitempty"`
	Timeouts     TimeoutConfig                 `json:"timeouts,omitempty"`
	RetryConfig  RetryConfig                   `json:"retry_config,omitempty"`
	Completed    bool                          `json:"completed,omitempty"`
	StageSync    chan PipelineStage            `json:"-"`                       // For stage synchronization
	StageRetries map[PipelineStage]int         `json:"stage_retries,omitempty"` // Track retries per stage
}

// WorkerJob represents any job that can be processed by a worker
type WorkerJob interface {
	GetID() string
	GetType() JobType
	GetGovIDs() []string
}

// Implement WorkerJob interface for all job types
func (j *BaseJob) GetID() string       { return j.ID }
func (j *BaseJob) GetType() JobType    { return j.Type }
func (j *BaseJob) GetGovIDs() []string { return j.GovIDs }

func (j *ScrapeJob) GetID() string       { return j.BaseJob.GetID() }
func (j *ScrapeJob) GetType() JobType    { return j.BaseJob.GetType() }
func (j *ScrapeJob) GetGovIDs() []string { return j.BaseJob.GetGovIDs() }

func (j *DokitoJob) GetID() string       { return j.BaseJob.GetID() }
func (j *DokitoJob) GetType() JobType    { return j.BaseJob.GetType() }
func (j *DokitoJob) GetGovIDs() []string { return j.BaseJob.GetGovIDs() }

func (j *CaseListJob) GetID() string       { return j.BaseJob.GetID() }
func (j *CaseListJob) GetType() JobType    { return j.BaseJob.GetType() }
func (j *CaseListJob) GetGovIDs() []string { return j.BaseJob.GetGovIDs() }

func (j *PipelineJob) GetID() string       { return j.BaseJob.GetID() }
func (j *PipelineJob) GetType() JobType    { return j.BaseJob.GetType() }
func (j *PipelineJob) GetGovIDs() []string { return j.BaseJob.GetGovIDs() }

// WorkerResult represents the result of a worker job
type WorkerResult struct {
	JobID     string        `json:"job_id"`
	JobType   JobType       `json:"job_type"`
	GovID     string        `json:"gov_id"`
	Result    interface{}   `json:"result"`
	Error     string        `json:"error,omitempty"`
	Success   bool          `json:"success"`
	Stage     PipelineStage `json:"stage,omitempty"` // For pipeline jobs
	Timestamp time.Time     `json:"timestamp"`
	Logs      string        `json:"logs,omitempty"` // Container stdout/stderr
}

// StreamLogger is an io.Writer that logs output in real-time
type StreamLogger struct {
	workerID string
	jobID    string
	buffer   []byte
}

func (sl *StreamLogger) Write(p []byte) (n int, err error) {
	sl.buffer = append(sl.buffer, p...)

	// Log complete lines
	for {
		idx := bytes.IndexByte(sl.buffer, '\n')
		if idx == -1 {
			break
		}
		line := string(sl.buffer[:idx])
		sl.buffer = sl.buffer[idx+1:]

		if line != "" {
			log.Printf("[Worker %s Job %s] %s", sl.workerID, sl.jobID, line)
		}
	}

	return len(p), nil
}

// Worker represents a single worker instance
type Worker struct {
	ID            string
	Client        *client.Client
	Results       chan WorkerResult
	ctx           context.Context
	cancel        context.CancelFunc
	ScraperImage  string
	WorkDir       string
	ContainerID   string // Persistent scraper container ID
	ContainerName string // Container name for easy lookup
}

// Runner manages multiple workers and collects results
type Runner struct {
	workers       []*Worker
	results       map[string]interface{}
	resultsMux    sync.RWMutex
	jobQueue      chan WorkerJob
	resultQueue   chan WorkerResult
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	scraperImage  string
	workDir       string
	resultsDir    string
	dokitoBaseURL string

	// Dokito backend container management
	dokitoClient      *client.Client
	dokitoContainerID string

	// Queue management
	queueState     *QueueState
	loadBalancer   *LoadBalancer
	queueStatePath string
	autoSaveQueue  bool

	// Queue worker configuration
	autoProcessQueue  bool
	queueBatchSize    int
	queuePollInterval time.Duration
	queueWakeupChan   chan struct{} // Channel to wake up queue worker immediately

	// Caselist polling
	caselistPollerCancel context.CancelFunc

	// API server
	httpServer *http.Server
	apiPort    int
	apiEnabled bool
}

// ContainerConfig holds configuration for creating containers
type ContainerConfig struct {
	Image       string
	GovIDs      []string
	Mode        string
	OutputDir   string
	Environment []string
}

// getDockerSocketPath returns the Docker socket path from environment or default
func getDockerSocketPath() string {
	if socketPath := os.Getenv("DOCKER_SOCKET_PATH"); socketPath != "" {
		return socketPath
	}

	// Use the macOS Docker Desktop socket path
	return "unix:///Users/orchid/.docker/run/docker.sock"
}

// CreateScraperContainer creates a new Docker container for scraping with volume mounting
func CreateScraperContainer(config ContainerConfig) (string, error) {
	dockerSocket := getDockerSocketPath()
	log.Printf("Using Docker socket: %s", dockerSocket)

	cli, err := client.NewClientWithOpts(
		client.WithHost(dockerSocket),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return "", fmt.Errorf("unable to create docker client: %w", err)
	}

	// Ensure output directory exists
	log.Printf("Creating output directory: %s", config.OutputDir)
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		log.Printf("Failed to create output directory %s: %v", config.OutputDir, err)
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	// Verify directory exists
	if _, err := os.Stat(config.OutputDir); err != nil {
		log.Printf("Output directory verification failed: %v", err)
		return "", fmt.Errorf("output directory verification failed: %w", err)
	}
	log.Printf("✅ Output directory created and verified: %s", config.OutputDir)

	// Convert to absolute path for Docker mount
	absOutputDir, err := filepath.Abs(config.OutputDir)
	if err != nil {
		log.Printf("Failed to convert output directory to absolute path: %v", err)
		return "", fmt.Errorf("failed to convert output directory to absolute path: %w", err)
	}

	// Prepare environment variables
	env := append(config.Environment,
		fmt.Sprintf("GOVIDS=%s", strings.Join(config.GovIDs, ",")),
		fmt.Sprintf("MODE=%s", config.Mode),
		"OUTPUT_DIR=/app/output",
	)

	// Use Binds instead of Mounts for better Docker Desktop compatibility
	bindMount := fmt.Sprintf("%s:/app/output", absOutputDir)

	// Create container
	log.Printf("Creating container with image: %s, govIDs: %v, mode: %s", config.Image, config.GovIDs, config.Mode)
	log.Printf("Container environment: %v", env)
	log.Printf("Container bind mount: %s", bindMount)

	cont, err := cli.ContainerCreate(
		context.Background(),
		&container.Config{
			Image:      config.Image,
			Env:        env,
			Cmd:        []string{"node", "scraper-container.js"},
			WorkingDir: "/app",
		},
		&container.HostConfig{
			Binds:       []string{bindMount},
			AutoRemove:  false, // We'll remove manually after collecting results
			NetworkMode: "bridge",
		},
		nil, nil, "")
	if err != nil {
		log.Printf("Container creation failed with error: %v", err)
		return "", fmt.Errorf("container create failed for image %s: %w", config.Image, err)
	}

	log.Printf("Container created successfully: %s", cont.ID[:12])

	// Start container
	log.Printf("Starting container %s...", cont.ID[:12])
	err = cli.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		log.Printf("Container start failed: %v", err)
		return "", fmt.Errorf("container start failed: %w", err)
	}

	log.Printf("✅ Scraper container %s started successfully with output dir %s", cont.ID[:12], config.OutputDir)
	return cont.ID, nil
}

// WaitForContainer waits for a container to complete and returns exit code
func WaitForContainer(cli *client.Client, containerID string, timeout time.Duration) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	statusCh, errCh := cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return -1, fmt.Errorf("error waiting for container: %w", err)
		}
	case status := <-statusCh:
		return status.StatusCode, nil
	case <-ctx.Done():
		return -1, fmt.Errorf("timeout waiting for container %s", containerID[:12])
	}
	return -1, fmt.Errorf("unexpected error waiting for container")
}

// WaitForContainerWithLogs waits for a container to complete and captures stdout/stderr
func WaitForContainerWithLogs(cli *client.Client, containerID string, timeout time.Duration) (int64, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Start capturing logs immediately
	logOptions := types.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
		Timestamps: true,
	}

	logReader, err := cli.ContainerLogs(ctx, containerID, logOptions)
	if err != nil {
		return -1, "", fmt.Errorf("failed to get container logs: %w", err)
	}
	defer logReader.Close()

	// Use separate buffers for stdout and stderr
	var stdoutBuf, stderrBuf bytes.Buffer
	logsChan := make(chan string, 1)
	logErrChan := make(chan error, 1)
	logDoneChan := make(chan struct{}, 1)

	// Stream logs in real-time with proper demultiplexing
	go func() {
		defer close(logsChan)
		defer close(logErrChan)
		defer close(logDoneChan)

		// Docker multiplexes stdout and stderr into a single stream
		// We need to demultiplex it using stdcopy.StdCopy
		_, err := stdcopy.StdCopy(&stdoutBuf, &stderrBuf, logReader)
		if err != nil && err != io.EOF {
			logErrChan <- fmt.Errorf("failed to demultiplex container logs: %w", err)
			return
		}

		// Combine stdout and stderr into a single log string
		var combinedLogs bytes.Buffer

		// Always include a header to distinguish between empty logs and no logs
		combinedLogs.WriteString("=== CONTAINER LOGS ===\n")

		if stdoutBuf.Len() > 0 {
			combinedLogs.WriteString("=== STDOUT ===\n")
			combinedLogs.Write(stdoutBuf.Bytes())
			combinedLogs.WriteString("\n")
		} else {
			combinedLogs.WriteString("=== STDOUT ===\n(empty)\n")
		}

		if stderrBuf.Len() > 0 {
			combinedLogs.WriteString("=== STDERR ===\n")
			combinedLogs.Write(stderrBuf.Bytes())
			combinedLogs.WriteString("\n")
		} else {
			combinedLogs.WriteString("=== STDERR ===\n(empty)\n")
		}

		combinedLogs.WriteString("=== END LOGS ===\n")

		// Always send something to logsChan, even if logs are empty
		logsChan <- combinedLogs.String()
	}()

	// Wait for container completion
	statusCh, errCh := cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)

	var exitCode int64
	var logs string

	// Wait for container completion first
	select {
	case err := <-errCh:
		if err != nil {
			return -1, "", fmt.Errorf("error waiting for container: %w", err)
		}
	case status := <-statusCh:
		exitCode = status.StatusCode
	case <-ctx.Done():
		return -1, "", fmt.Errorf("timeout waiting for container %s", containerID[:12])
	}

	// After container completion, wait for logs with increased timeout
	select {
	case logs = <-logsChan:
		// Successfully got logs - even if empty, this is success
		if logs == "" {
			logs = "=== CONTAINER LOGS ===\n=== STDOUT ===\n(empty)\n=== STDERR ===\n(empty)\n=== END LOGS ===\n"
		}
	case err := <-logErrChan:
		if err != nil {
			log.Printf("Warning: Failed to capture logs for container %s: %v", containerID[:12], err)
			logs = fmt.Sprintf("=== CONTAINER LOGS ===\nLog capture failed: %v\n=== END LOGS ===\n", err)
		} else {
			log.Printf("Warning: Failed to capture logs for container %s: received nil error", containerID[:12])
			logs = "=== CONTAINER LOGS ===\nLog capture failed: received nil error\n=== END LOGS ===\n"
		}
	case <-time.After(15 * time.Second): // Increased timeout from 5 to 15 seconds
		log.Printf("Warning: Timeout capturing logs for container %s", containerID[:12])
		// Try to get partial logs
		if stdoutBuf.Len() > 0 || stderrBuf.Len() > 0 {
			var partialLogs bytes.Buffer
			partialLogs.WriteString("=== CONTAINER LOGS ===\n")
			partialLogs.WriteString("Log capture timed out - partial logs:\n")
			if stdoutBuf.Len() > 0 {
				partialLogs.WriteString("=== STDOUT (partial) ===\n")
				partialLogs.Write(stdoutBuf.Bytes())
				partialLogs.WriteString("\n")
			}
			if stderrBuf.Len() > 0 {
				partialLogs.WriteString("=== STDERR (partial) ===\n")
				partialLogs.Write(stderrBuf.Bytes())
				partialLogs.WriteString("\n")
			}
			partialLogs.WriteString("=== END LOGS ===\n")
			logs = partialLogs.String()
		} else {
			logs = "=== CONTAINER LOGS ===\nLog capture timed out - no logs captured\n=== END LOGS ===\n"
		}
	}

	return exitCode, logs, nil
}

// CollectContainerResults reads JSON results from the container's output directory
func CollectContainerResults(outputDir string) (map[string]interface{}, error) {
	files, err := ioutil.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read output directory: %w", err)
	}

	results := make(map[string]interface{})
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			filePath := filepath.Join(outputDir, file.Name())
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				log.Printf("Warning: failed to read %s: %v", filePath, err)
				continue
			}

			var jsonData interface{}
			if err := json.Unmarshal(data, &jsonData); err != nil {
				log.Printf("Warning: failed to parse JSON in %s: %v", filePath, err)
				continue
			}

			// Use filename (without extension) as key
			key := strings.TrimSuffix(file.Name(), ".json")
			results[key] = jsonData
		}
	}

	return results, nil
}

// CleanupContainer removes a container and its associated resources
func CleanupContainer(cli *client.Client, containerID string, outputDir string) error {
	// Remove container
	err := cli.ContainerRemove(context.Background(), containerID, types.ContainerRemoveOptions{
		Force: true,
	})
	if err != nil {
		log.Printf("Warning: failed to remove container %s: %v", containerID[:12], err)
	}

	// Clean up output directory
	if err := os.RemoveAll(outputDir); err != nil {
		log.Printf("Warning: failed to clean up output directory %s: %v", outputDir, err)
	}

	return nil
}

// findWorkerScraperContainer searches for an existing scraper container for a specific worker
// Returns: (containerID, isRunning, exists)
func (w *Worker) findWorkerScraperContainer() (string, bool, bool) {
	// List ALL containers including stopped ones
	containers, err := w.Client.ContainerList(context.Background(), types.ContainerListOptions{All: true})
	if err != nil {
		log.Printf("Worker %s: Failed to list containers: %v", w.ID, err)
		return "", false, false
	}

	for _, container := range containers {
		// Check by container name
		for _, name := range container.Names {
			// Docker prepends "/" to container names
			cleanName := strings.TrimPrefix(name, "/")
			if cleanName == w.ContainerName {
				isRunning := container.State == "running"
				log.Printf("Worker %s: Found existing container %s (running: %v)", w.ID, container.ID[:12], isRunning)
				return container.ID, isRunning, true
			}
		}
	}

	log.Printf("Worker %s: No existing container found with name %s", w.ID, w.ContainerName)
	return "", false, false
}

// ensureScraperContainer ensures a persistent scraper container is running for this worker
// Handles three scenarios: running container, stopped container, or no container
func (w *Worker) ensureScraperContainer() error {
	containerID, isRunning, exists := w.findWorkerScraperContainer()

	if exists {
		w.ContainerID = containerID
		if isRunning {
			log.Printf("✅ Worker %s: Found running scraper container: %s", w.ID, containerID[:12])
			return nil
		} else {
			log.Printf("▶️  Worker %s: Starting stopped scraper container: %s", w.ID, containerID[:12])
			err := w.Client.ContainerStart(context.Background(), containerID, types.ContainerStartOptions{})
			if err != nil {
				return fmt.Errorf("failed to start existing scraper container: %w", err)
			}
			log.Printf("✅ Worker %s: Scraper container started: %s", w.ID, containerID[:12])
			return nil
		}
	}

	// Container doesn't exist - create new one
	return w.createWorkerScraperContainer()
}

// createWorkerScraperContainer creates a new persistent scraper container for this worker
func (w *Worker) createWorkerScraperContainer() error {
	log.Printf("🔨 Worker %s: Creating persistent scraper container %s", w.ID, w.ContainerName)

	// Create persistent output directory for this worker
	workerOutputDir := filepath.Join(w.WorkDir, "output")
	if err := os.MkdirAll(workerOutputDir, 0755); err != nil {
		return fmt.Errorf("failed to create worker output directory: %w", err)
	}

	absOutputDir, err := filepath.Abs(workerOutputDir)
	if err != nil {
		return fmt.Errorf("failed to convert output directory to absolute path: %w", err)
	}

	// Create volume mount for output
	mounts := []mount.Mount{
		{
			Type:   mount.TypeBind,
			Source: absOutputDir,
			Target: "/app/output",
		},
	}

	// Create container that stays alive
	// We'll use "tail -f /dev/null" to keep it running and use docker exec for jobs
	cont, err := w.Client.ContainerCreate(
		context.Background(),
		&container.Config{
			Image: w.ScraperImage,
			Env: []string{
				fmt.Sprintf("WORKER_ID=%s", w.ID),
			},
			Cmd:        []string{"tail", "-f", "/dev/null"}, // Keep container alive
			WorkingDir: "/app",
		},
		&container.HostConfig{
			Mounts:      mounts,
			AutoRemove:  false, // We manage lifecycle
			NetworkMode: "bridge",
			RestartPolicy: container.RestartPolicy{
				Name: "unless-stopped", // Auto-restart if it crashes
			},
		},
		nil, nil, w.ContainerName)
	if err != nil {
		return fmt.Errorf("failed to create scraper container: %w", err)
	}

	w.ContainerID = cont.ID
	log.Printf("📦 Worker %s: Container created: %s", w.ID, cont.ID[:12])

	// Start container
	err = w.Client.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start scraper container: %w", err)
	}

	log.Printf("✅ Worker %s: Persistent scraper container %s started successfully", w.ID, w.ContainerName)
	return nil
}

// stopScraperContainer stops and removes the worker's persistent container
func (w *Worker) stopScraperContainer() error {
	if w.ContainerID == "" {
		return nil
	}

	log.Printf("🛑 Worker %s: Stopping scraper container: %s", w.ID, w.ContainerID[:12])

	// Stop container
	timeoutSeconds := 10
	err := w.Client.ContainerStop(context.Background(), w.ContainerID, container.StopOptions{
		Timeout: &timeoutSeconds,
	})
	if err != nil {
		log.Printf("Warning: Worker %s failed to stop container: %v", w.ID, err)
	}

	// Remove container
	err = w.Client.ContainerRemove(context.Background(), w.ContainerID, types.ContainerRemoveOptions{
		Force: true,
	})
	if err != nil {
		log.Printf("Warning: Worker %s failed to remove container: %v", w.ID, err)
	}

	log.Printf("✅ Worker %s: Scraper container removed", w.ID)
	w.ContainerID = ""
	return nil
}

// findDokitoBackendContainer searches for an existing dokito-backend container
// Returns: (containerID, isRunning, exists)
func (r *Runner) findDokitoBackendContainer() (string, bool, bool) {
	// List ALL containers including stopped ones
	containers, err := r.dokitoClient.ContainerList(context.Background(), types.ContainerListOptions{All: true})
	if err != nil {
		log.Printf("Failed to list containers: %v", err)
		return "", false, false
	}

	for _, container := range containers {
		// Check by container name
		for _, name := range container.Names {
			if strings.Contains(name, "dokito-backend-runner") || strings.Contains(name, "dokito-backend") {
				isRunning := container.State == "running"
				return container.ID, isRunning, true
			}
		}
		// Also check by image name
		if strings.Contains(container.Image, "dokito-backend") || strings.Contains(container.Image, "jobrunner-dokito-backend") {
			isRunning := container.State == "running"
			return container.ID, isRunning, true
		}
	}

	return "", false, false
}

// isDokitoBackendRunning checks if a dokito-backend container is already running
func (r *Runner) isDokitoBackendRunning() bool {
	containers, err := r.dokitoClient.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		log.Printf("Failed to list containers: %v", err)
		return false
	}

	for _, container := range containers {
		for _, name := range container.Names {
			if strings.Contains(name, "dokito-backend") {
				// Check if container is actually running
				if container.State == "running" {
					r.dokitoContainerID = container.ID
					return true
				}
			}
		}
		// Also check by image name
		if (strings.Contains(container.Image, "dokito-backend") || strings.Contains(container.Image, "jobrunner-dokito-backend")) && container.State == "running" {
			r.dokitoContainerID = container.ID
			return true
		}
	}
	return false
}

// replaceLocalhostWithDockerHost replaces localhost/127.0.0.1 with host.docker.internal for bridge networking
func replaceLocalhostWithDockerHost(value string) string {
	// Only replace localhost/127.0.0.1 addresses, not external hosts
	if strings.Contains(value, "127.0.0.1") {
		value = strings.ReplaceAll(value, "127.0.0.1", "host.docker.internal")
	}
	if strings.Contains(value, "localhost") && !strings.Contains(value, ".") {
		// Only replace standalone "localhost", not domains containing "localhost"
		value = strings.ReplaceAll(value, "localhost", "host.docker.internal")
	}
	return value
}

// createDokitoBackend creates a new dokito-backend container
func (r *Runner) createDokitoBackend() error {
	log.Printf("🆕 Creating new dokito-backend container...")

	// Create container configuration
	containerConfig := &container.Config{
		Image: "jobrunner-dokito-backend:latest",
		Env: []string{
			"PORT=8123",
			fmt.Sprintf("DATABASE_URL=%s", replaceLocalhostWithDockerHost(os.Getenv("DATABASE_URL"))),
			fmt.Sprintf("SUPABASE_URL=%s", replaceLocalhostWithDockerHost(os.Getenv("SUPABASE_URL"))),
			fmt.Sprintf("SUPABASE_ANON_KEY=%s", os.Getenv("SUPABASE_ANON_KEY")),
			fmt.Sprintf("SUPABASE_SERVICE_ROLE_KEY=%s", os.Getenv("SUPABASE_SERVICE_ROLE_KEY")),
			fmt.Sprintf("OPENSCRAPERS_S3_OBJECT_BUCKET=%s", os.Getenv("OPENSCRAPERS_S3_OBJECT_BUCKET")),
			fmt.Sprintf("DIGITALOCEAN_S3_CLOUD_REGION=%s", os.Getenv("DIGITALOCEAN_S3_CLOUD_REGION")),
			fmt.Sprintf("DIGITALOCEAN_S3_ENDPOINT=%s", replaceLocalhostWithDockerHost(os.Getenv("DIGITALOCEAN_S3_ENDPOINT"))),
			fmt.Sprintf("DIGITALOCEAN_S3_ACCESS_KEY=%s", os.Getenv("DIGITALOCEAN_S3_ACCESS_KEY")),
			fmt.Sprintf("DIGITALOCEAN_S3_SECRET_KEY=%s", os.Getenv("DIGITALOCEAN_S3_SECRET_KEY")),
			fmt.Sprintf("DEEPINFRA_API_KEY=%s", os.Getenv("DEEPINFRA_API_KEY")),
			"PUBLIC_SAFE_MODE=false",
		},
		ExposedPorts: nat.PortSet{
			"8123/tcp": struct{}{},
		},
	}

	hostConfig := &container.HostConfig{
		// Use port bindings for cross-platform compatibility (macOS Docker Desktop requirement)
		PortBindings: nat.PortMap{
			"8123/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: "8123",
				},
			},
		},
		RestartPolicy: container.RestartPolicy{
			Name: "unless-stopped",
		},
	}

	// Create container
	resp, err := r.dokitoClient.ContainerCreate(context.Background(), containerConfig, hostConfig, nil, nil, "dokito-backend-runner")
	if err != nil {
		return fmt.Errorf("failed to create dokito-backend container: %w", err)
	}

	r.dokitoContainerID = resp.ID

	// Start container
	err = r.dokitoClient.ContainerStart(context.Background(), r.dokitoContainerID, types.ContainerStartOptions{})
	if err != nil {
		return fmt.Errorf("failed to start dokito-backend container: %w", err)
	}

	log.Printf("✅ Dokito-backend container created and started with ID: %s", r.dokitoContainerID[:12])

	// Give the container a moment to start up before health checks
	time.Sleep(2 * time.Second)

	// Check if container is still running
	containerInfo, err := r.dokitoClient.ContainerInspect(context.Background(), r.dokitoContainerID)
	if err != nil {
		log.Printf("⚠️ Warning: could not inspect container: %v", err)
	} else {
		healthStatus := "unknown"
		if containerInfo.State.Health != nil {
			healthStatus = containerInfo.State.Health.Status
		}
		log.Printf("📊 Container state: %s, health: %s", containerInfo.State.Status, healthStatus)
		if !containerInfo.State.Running {
			log.Printf("❌ Container exited with code: %d", containerInfo.State.ExitCode)
			// Get logs to see why it failed
			logs, _ := r.dokitoClient.ContainerLogs(context.Background(), r.dokitoContainerID, types.ContainerLogsOptions{
				ShowStdout: true,
				ShowStderr: true,
				Tail:       "20",
			})
			if logs != nil {
				logData, _ := io.ReadAll(logs)
				log.Printf("📋 Container startup logs:\n%s", string(logData))
				logs.Close()
			}
		}
	}

	return nil
}

// ensureDokitoBackend ensures a dokito-backend container is running
// Handles three scenarios: running container, stopped container, or no container
func (r *Runner) ensureDokitoBackend() error {
	containerID, isRunning, exists := r.findDokitoBackendContainer()

	if exists {
		r.dokitoContainerID = containerID
		if isRunning {
			log.Printf("✅ Found running dokito-backend container: %s", containerID[:12])
			return nil
		} else {
			log.Printf("▶️  Starting stopped dokito-backend container: %s", containerID[:12])
			err := r.dokitoClient.ContainerStart(context.Background(), containerID, types.ContainerStartOptions{})
			if err != nil {
				return fmt.Errorf("failed to start existing dokito-backend container: %w", err)
			}
			log.Printf("✅ Dokito-backend container started: %s", containerID[:12])

			// Give container a moment to start
			time.Sleep(2 * time.Second)
			return nil
		}
	}

	// Container doesn't exist - create new one
	return r.createDokitoBackend()
}

// startDokitoBackend starts the dokito-backend container (deprecated, use ensureDokitoBackend)
func (r *Runner) startDokitoBackend() error {
	return r.ensureDokitoBackend()
}

// stopDokitoBackend stops and removes the dokito-backend container
func (r *Runner) stopDokitoBackend() error {
	if r.dokitoContainerID == "" {
		return nil
	}

	log.Printf("🛑 Stopping dokito-backend container: %s", r.dokitoContainerID[:12])

	// Stop container
	timeoutSeconds := 10
	err := r.dokitoClient.ContainerStop(context.Background(), r.dokitoContainerID, container.StopOptions{
		Timeout: &timeoutSeconds,
	})
	if err != nil {
		log.Printf("Warning: failed to stop dokito-backend container: %v", err)
	}

	// Remove container
	err = r.dokitoClient.ContainerRemove(context.Background(), r.dokitoContainerID, types.ContainerRemoveOptions{
		Force: true,
	})
	if err != nil {
		log.Printf("Warning: failed to remove dokito-backend container: %v", err)
	}

	r.dokitoContainerID = ""
	return nil
}

// waitForDokitoBackend waits for the dokito-backend to be ready
func (r *Runner) waitForDokitoBackend() error {
	log.Printf("⏳ Waiting for dokito-backend to be ready at %s...", r.dokitoBaseURL)

	timeout := time.After(90 * time.Second)   // Increased timeout
	ticker := time.NewTicker(3 * time.Second) // Check less frequently
	defer ticker.Stop()

	attempts := 0
	for {
		select {
		case <-timeout:
			// Log container status before giving up
			if r.dokitoContainerID != "" {
				logs, _ := r.dokitoClient.ContainerLogs(context.Background(), r.dokitoContainerID, types.ContainerLogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Tail:       "50",
				})
				if logs != nil {
					logData, _ := io.ReadAll(logs)
					log.Printf("📋 Container logs (last 50 lines):\n%s", string(logData))
					logs.Close()
				}
			}
			return fmt.Errorf("dokito-backend health check timed out after 90 seconds (tried %d times)", attempts)
		case <-ticker.C:
			attempts++
			log.Printf("🔍 Health check attempt %d: trying %s/health", attempts, r.dokitoBaseURL)

			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(r.dokitoBaseURL + "/health")
			if err != nil {
				log.Printf("❌ Health check failed: %v", err)
				continue
			}

			if resp.StatusCode == 200 {
				resp.Body.Close()
				log.Printf("✅ Dokito-backend is ready at %s (attempt %d)", r.dokitoBaseURL, attempts)
				return nil
			}

			log.Printf("⚠️ Health check returned status %d", resp.StatusCode)
			resp.Body.Close()
		}
	}
}

// NewRunner creates a new runner with specified number of workers
func NewRunner(workerCount int, scraperImage string, workDir string, dokitoBaseURL string) (*Runner, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Ensure work directory exists
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create work directory: %w", err)
	}

	// Create persistent results directory
	resultsDir := filepath.Join(workDir, "results")
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create results directory: %w", err)
	}

	// Create Docker client for dokito-backend management
	dockerSocket := getDockerSocketPath()
	dokitoClient, err := client.NewClientWithOpts(
		client.WithHost(dockerSocket),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create docker client for dokito-backend: %w", err)
	}

	// Initialize queue state
	queueStatePath := filepath.Join(workDir, "queue_state.json")
	queueState := NewQueueState("main-queue")

	// Try to load existing queue state
	if _, err := os.Stat(queueStatePath); err == nil {
		log.Printf("📂 Found existing queue state, loading...")
		loadedState, err := LoadQueueStateFromFile(queueStatePath)
		if err != nil {
			log.Printf("⚠️  Failed to load queue state: %v, starting with fresh queue", err)
		} else {
			queueState = loadedState
			log.Printf("✅ Loaded queue state: %d pending, %d processing, %d completed, %d failed",
				len(queueState.PendingGovIDs), len(queueState.ProcessingGovIDs),
				len(queueState.CompletedGovIDs), len(queueState.FailedGovIDs))
		}
	}

	runner := &Runner{
		workers:           make([]*Worker, 0, workerCount),
		results:           make(map[string]interface{}),
		jobQueue:          make(chan WorkerJob, 100),
		resultQueue:       make(chan WorkerResult, 100),
		ctx:               ctx,
		cancel:            cancel,
		scraperImage:      scraperImage,
		workDir:           workDir,
		resultsDir:        resultsDir,
		dokitoBaseURL:     dokitoBaseURL,
		dokitoClient:      dokitoClient,
		queueState:        queueState,
		queueStatePath:    queueStatePath,
		autoSaveQueue:     true,                    // Enable auto-save by default
		autoProcessQueue:  true,                    // Enable auto-processing by default
		queueBatchSize:    6,                       // Process 6 govIDs at a time
		queuePollInterval: 5 * time.Second,         // Check queue every 5 seconds
		queueWakeupChan:   make(chan struct{}, 10), // Buffered channel for wake-up signals
		apiEnabled:        true,                    // Enable API by default
	}

	// Ensure dokito-backend is running (handles existing, stopped, or missing containers)
	if err := runner.ensureDokitoBackend(); err != nil {
		runner.Stop()
		return nil, fmt.Errorf("failed to ensure dokito-backend is running: %w", err)
	}

	// Wait for dokito-backend to be ready
	if err := runner.waitForDokitoBackend(); err != nil {
		runner.Stop()
		return nil, fmt.Errorf("dokito-backend failed to become ready: %w", err)
	}

	// Create workers
	workerIDs := make([]string, 0, workerCount)
	for i := 0; i <= workerCount; i++ {
		worker, err := runner.createWorker(i)
		if err != nil {
			runner.Stop()
			return nil, fmt.Errorf("failed to create worker %d: %w", i, err)
		}
		runner.workers = append(runner.workers, worker)
		workerIDs = append(workerIDs, worker.ID)
	}

	// Initialize load balancer with worker IDs
	runner.loadBalancer = NewLoadBalancer(workerIDs, LeastLoaded)
	log.Printf("✅ Load balancer initialized with %d workers using %s strategy", len(workerIDs), LeastLoaded)

	// Start result collector
	go runner.collectResults()

	return runner, nil
}

// createWorker creates a new worker instance
func (r *Runner) createWorker(id int) (*Worker, error) {
	dockerSocket := getDockerSocketPath()
	cli, err := client.NewClientWithOpts(
		client.WithHost(dockerSocket),
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to create docker client: %w", err)
	}

	ctx, cancel := context.WithCancel(r.ctx)

	// Create worker-specific work directory
	workerDir := filepath.Join(r.workDir, fmt.Sprintf("worker-%d", id))
	if err := os.MkdirAll(workerDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create worker directory: %w", err)
	}

	workerID := fmt.Sprintf("worker-%d", id)
	worker := &Worker{
		ID:            workerID,
		Client:        cli,
		Results:       make(chan WorkerResult, 10),
		ctx:           ctx,
		cancel:        cancel,
		ScraperImage:  r.scraperImage,
		WorkDir:       workerDir,
		ContainerName: fmt.Sprintf("scraper-%s", workerID),
	}

	// Start worker goroutine with shared job queue
	go worker.run(r.jobQueue, r.resultQueue, r.dokitoBaseURL)

	return worker, nil
}

// run executes the worker's main loop, pulling jobs from a shared queue
func (w *Worker) run(jobQueue <-chan WorkerJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Worker %s starting (using ephemeral containers)", w.ID)

	for {
		select {
		case <-w.ctx.Done():
			log.Printf("Worker %s stopping", w.ID)
			return
		case job := <-jobQueue:
			log.Printf("Worker %s picked up job %s", w.ID, job.GetID())
			w.processJob(job, resultQueue, dokitoBaseURL)
		}
	}
}

// processJob processes a single job based on its type
func (w *Worker) processJob(job WorkerJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Worker %s processing job %s of type %s", w.ID, job.GetID(), job.GetType())

	// Route to appropriate processing method based on job type
	switch job.GetType() {
	case JobTypeScrape:
		if scrapeJob, ok := job.(*ScrapeJob); ok {
			w.processScrapeJob(scrapeJob, resultQueue)
		} else {
			w.sendErrorResult(job.GetID(), job.GetType(), "", "Invalid scrape job type", resultQueue)
		}
	case JobTypeDokito:
		if dokitoJob, ok := job.(*DokitoJob); ok {
			w.processDokitoJob(dokitoJob, resultQueue, dokitoBaseURL)
		} else {
			w.sendErrorResult(job.GetID(), job.GetType(), "", "Invalid dokito job type", resultQueue)
		}
	case JobTypePipeline:
		if pipelineJob, ok := job.(*PipelineJob); ok {
			w.processPipelineJob(pipelineJob, resultQueue, dokitoBaseURL)
		} else {
			w.sendErrorResult(job.GetID(), job.GetType(), "", "Invalid pipeline job type", resultQueue)
		}
	case JobTypeCaseList:
		if caseListJob, ok := job.(*CaseListJob); ok {
			w.processCaseListJob(caseListJob, resultQueue)
		} else {
			w.sendErrorResult(job.GetID(), job.GetType(), "", "Invalid caselist job type", resultQueue)
		}
	default:
		w.sendErrorResult(job.GetID(), job.GetType(), "", fmt.Sprintf("Unknown job type: %s", job.GetType()), resultQueue)
	}
}

// sendErrorResult sends an error result to the result queue
func (w *Worker) sendErrorResult(jobID string, jobType JobType, govID string, errorMsg string, resultQueue chan<- WorkerResult) {
	resultQueue <- WorkerResult{
		JobID:     jobID,
		JobType:   jobType,
		GovID:     govID,
		Result:    nil,
		Error:     errorMsg,
		Success:   false,
		Timestamp: time.Now(),
	}
}

func (w *Worker) sendErrorResultWithLogs(jobID string, jobType JobType, govID string, errorMsg string, logs string, resultQueue chan<- WorkerResult) {
	resultQueue <- WorkerResult{
		JobID:     jobID,
		JobType:   jobType,
		GovID:     govID,
		Result:    nil,
		Error:     errorMsg,
		Success:   false,
		Timestamp: time.Now(),
		Logs:      logs,
	}
}

// processScrapeJobWithPersistentContainer executes a scrape job using docker exec in the persistent container
func (w *Worker) processScrapeJobWithPersistentContainer(job *ScrapeJob, resultQueue chan<- WorkerResult) {
	log.Printf("🚀 Worker %s: Using persistent container %s for job %s", w.ID, w.ContainerID[:12], job.ID)

	// Create job-specific subdirectory in the persistent output mount
	jobOutputSubdir := fmt.Sprintf("job-%s-%d", job.ID, time.Now().Unix())
	hostJobOutputDir := filepath.Join(w.WorkDir, "output", jobOutputSubdir)
	containerJobOutputPath := filepath.Join("/app/output", jobOutputSubdir)

	// Create the job output directory on the host (it's mounted into the container)
	if err := os.MkdirAll(hostJobOutputDir, 0755); err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "setup", fmt.Sprintf("Failed to create job output directory: %v", err), resultQueue)
		return
	}

	// Build environment variables for the exec command
	envVars := []string{
		fmt.Sprintf("GOVIDS=%s", strings.Join(job.GovIDs, ",")),
		fmt.Sprintf("MODE=%s", job.Mode),
		fmt.Sprintf("OUTPUT_DIR=%s", containerJobOutputPath),
		fmt.Sprintf("JOB_ID=%s", job.ID),
		fmt.Sprintf("WORKER_ID=%s", w.ID),
	}
	if job.DateString != "" {
		envVars = append(envVars, fmt.Sprintf("DATE_STRING=%s", job.DateString))
	}
	if job.BeginDate != "" {
		envVars = append(envVars, fmt.Sprintf("BEGIN_DATE=%s", job.BeginDate))
	}
	if job.EndDate != "" {
		envVars = append(envVars, fmt.Sprintf("END_DATE=%s", job.EndDate))
	}

	// Create exec config to run the scraper inside the persistent container
	execConfig := types.ExecConfig{
		Cmd:          []string{"node", "scraper-container.js"},
		Env:          envVars,
		WorkingDir:   "/app",
		AttachStdout: true,
		AttachStderr: true,
	}

	// Create exec instance
	execID, err := w.Client.ContainerExecCreate(context.Background(), w.ContainerID, execConfig)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "exec", fmt.Sprintf("Failed to create exec: %v", err), resultQueue)
		return
	}

	// Start exec and attach to capture output
	resp, err := w.Client.ContainerExecAttach(context.Background(), execID.ID, types.ExecStartCheck{})
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "exec", fmt.Sprintf("Failed to start exec: %v", err), resultQueue)
		return
	}
	defer resp.Close()

	// Read logs from exec output
	var logBuffer bytes.Buffer
	_, err = stdcopy.StdCopy(&logBuffer, &logBuffer, resp.Reader)
	if err != nil && err != io.EOF {
		log.Printf("Warning: Error reading exec output: %v", err)
	}
	containerLogs := logBuffer.String()

	// Wait for exec to complete with timeout
	containerTimeout := 10 * time.Minute
	if timeouts := GetDefaultTimeouts(); timeouts.ContainerTimeoutMinutes > 0 {
		containerTimeout = time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	// Poll for exec completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var exitCode int
	execCompleted := false

	for {
		select {
		case <-timeoutCtx.Done():
			w.sendErrorResult(job.ID, JobTypeScrape, "timeout", "Container exec timed out", resultQueue)
			return
		case <-ticker.C:
			inspect, err := w.Client.ContainerExecInspect(context.Background(), execID.ID)
			if err != nil {
				w.sendErrorResult(job.ID, JobTypeScrape, "inspect", fmt.Sprintf("Failed to inspect exec: %v", err), resultQueue)
				return
			}
			if !inspect.Running {
				exitCode = inspect.ExitCode
				execCompleted = true
				break
			}
		}
		if execCompleted {
			break
		}
	}

	if exitCode != 0 {
		w.sendErrorResultWithLogs(job.ID, JobTypeScrape, "container", fmt.Sprintf("Container exec exited with code %d", exitCode), containerLogs, resultQueue)
		return
	}

	// Collect results from the job output directory
	results, err := CollectContainerResults(hostJobOutputDir)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "results", fmt.Sprintf("Failed to collect results: %v", err), resultQueue)
		return
	}

	// Send results for each govID
	for _, govID := range job.GovIDs {
		govResult, exists := results[govID]
		if !exists {
			if genericResult, hasGeneric := results["result"]; hasGeneric {
				govResult = genericResult
			} else {
				govResult = map[string]interface{}{
					"error":             fmt.Sprintf("No result found for govID %s", govID),
					"available_results": getResultKeys(results),
				}
			}
		}

		resultQueue <- WorkerResult{
			JobID:     job.ID,
			JobType:   JobTypeScrape,
			GovID:     govID,
			Result:    govResult,
			Success:   exists || results["result"] != nil,
			Timestamp: time.Now(),
			Logs:      containerLogs,
		}
	}

	log.Printf("✅ Worker %s completed scrape job %s using persistent container", w.ID, job.ID)
}

// processScrapeJob processes NY PUC scraping jobs using ephemeral containers
func (w *Worker) processScrapeJob(job *ScrapeJob, resultQueue chan<- WorkerResult) {
	log.Printf("Worker %s processing scrape job %s in mode %s for %d govIDs",
		w.ID, job.ID, job.Mode, len(job.GovIDs))

	// Create unique output directory for this job
	outputDir := filepath.Join(w.WorkDir, fmt.Sprintf("job-%s-%d", job.ID, time.Now().Unix()))

	// Configure container
	config := ContainerConfig{
		Image:     w.ScraperImage,
		GovIDs:    job.GovIDs,
		Mode:      string(job.Mode),
		OutputDir: outputDir,
		Environment: []string{
			fmt.Sprintf("JOB_ID=%s", job.ID),
			fmt.Sprintf("WORKER_ID=%s", w.ID),
		},
	}

	// Add date parameters if present
	if job.DateString != "" {
		config.Environment = append(config.Environment, fmt.Sprintf("DATE_STRING=%s", job.DateString))
	}
	if job.BeginDate != "" {
		config.Environment = append(config.Environment, fmt.Sprintf("BEGIN_DATE=%s", job.BeginDate))
	}
	if job.EndDate != "" {
		config.Environment = append(config.Environment, fmt.Sprintf("END_DATE=%s", job.EndDate))
	}

	// Create and start container
	containerID, err := CreateScraperContainer(config)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "container", fmt.Sprintf("Failed to create container: %v", err), resultQueue)
		return
	}
	defer CleanupContainer(w.Client, containerID, outputDir)

	// Wait for container to complete and capture logs with configurable timeout
	containerTimeout := 10 * time.Minute // Default timeout
	if timeouts := GetDefaultTimeouts(); timeouts.ContainerTimeoutMinutes > 0 {
		containerTimeout = time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	}
	exitCode, containerLogs, err := WaitForContainerWithLogs(w.Client, containerID, containerTimeout)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "container", fmt.Sprintf("Container execution failed: %v", err), resultQueue)
		return
	}

	if exitCode != 0 {
		w.sendErrorResultWithLogs(job.ID, JobTypeScrape, "container", fmt.Sprintf("Container exited with code %d", exitCode), containerLogs, resultQueue)
		return
	}

	// Collect results from output directory
	results, err := CollectContainerResults(outputDir)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeScrape, "results", fmt.Sprintf("Failed to collect results: %v", err), resultQueue)
		return
	}

	// Send results for each govID
	for _, govID := range job.GovIDs {
		govResult, exists := results[govID]
		if !exists {
			// If no specific result for this govID, try generic result file
			if genericResult, hasGeneric := results["result"]; hasGeneric {
				govResult = genericResult
			} else {
				govResult = map[string]interface{}{
					"error":             fmt.Sprintf("No result found for govID %s", govID),
					"available_results": getResultKeys(results),
				}
			}
		}

		resultQueue <- WorkerResult{
			JobID:     job.ID,
			JobType:   JobTypeScrape,
			GovID:     govID,
			Result:    govResult,
			Success:   exists || results["result"] != nil,
			Timestamp: time.Now(),
			Logs:      containerLogs,
		}
	}

	log.Printf("Worker %s completed scrape job %s", w.ID, job.ID)
}

// getResultKeys returns the keys of a results map for debugging
func getResultKeys(results map[string]interface{}) []string {
	keys := make([]string, 0, len(results))
	for k := range results {
		keys = append(keys, k)
	}
	return keys
}

// processPipelineJob processes pipeline workflow jobs with stage coordination
func (w *Worker) processPipelineJob(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Worker %s processing pipeline job %s with %d stages", w.ID, job.ID, len(job.Stages))

	// Initialize job if needed
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	if job.StageRetries == nil {
		job.StageRetries = make(map[PipelineStage]int)
	}
	if job.Timeouts.ScrapeTimeoutMinutes == 0 {
		job.Timeouts = GetDefaultTimeouts()
	}
	if job.RetryConfig.MaxRetries == 0 {
		job.RetryConfig = GetDefaultRetryConfig()
	}

	// Create stage synchronization channel
	job.StageSync = make(chan PipelineStage, len(job.Stages))

	// Execute all stages sequentially
	for i, stage := range job.Stages {
		log.Printf("Pipeline job %s executing stage %d/%d: %s", job.ID, i+1, len(job.Stages), stage)
		job.CurrentStage = stage

		// Execute the current stage with retry logic
		stageSuccess := false
		for attempt := 0; attempt <= job.RetryConfig.MaxRetries && !stageSuccess; attempt++ {
			if attempt > 0 {
				// Calculate backoff delay
				delay := w.calculateBackoffDelay(attempt, job.RetryConfig)
				log.Printf("Pipeline job %s stage %s attempt %d/%d failed, retrying in %v", job.ID, stage, attempt, job.RetryConfig.MaxRetries+1, delay)
				time.Sleep(delay)
				job.StageRetries[stage] = attempt
			}

			log.Printf("Pipeline job %s executing stage %s (attempt %d/%d)", job.ID, stage, attempt+1, job.RetryConfig.MaxRetries+1)

			var stageError error
			switch stage {
			case PipelineStageScrape:
				stageError = w.executeScrapeStageWithRetry(job, resultQueue)
			case PipelineStageUpload:
				stageError = w.executeUploadStageWithRetry(job, resultQueue, dokitoBaseURL)
			case PipelineStageProcess:
				stageError = w.executeProcessStageWithRetry(job, resultQueue, dokitoBaseURL)
			case PipelineStageIngest:
				stageError = w.executeIngestStageWithRetry(job, resultQueue, dokitoBaseURL)
			default:
				stageError = fmt.Errorf("unknown pipeline stage: %s", stage)
			}

			if stageError == nil {
				stageSuccess = true
				log.Printf("Pipeline job %s stage %s completed successfully on attempt %d", job.ID, stage, attempt+1)
			} else if !w.isRetryableError(stageError, job.RetryConfig) {
				// Non-retryable error, fail immediately
				log.Printf("Pipeline job %s stage %s failed with non-retryable error: %v", job.ID, stage, stageError)
				w.sendErrorResult(job.ID, JobTypePipeline, "pipeline", fmt.Sprintf("Stage %s failed: %v", stage, stageError), resultQueue)
				return
			} else if attempt >= job.RetryConfig.MaxRetries {
				// Exhausted all retries
				log.Printf("Pipeline job %s stage %s failed after %d attempts: %v", job.ID, stage, attempt+1, stageError)
				w.sendErrorResult(job.ID, JobTypePipeline, "pipeline", fmt.Sprintf("Stage %s failed after %d attempts: %v", stage, attempt+1, stageError), resultQueue)
				return
			}
		}

		if !stageSuccess {
			w.sendErrorResult(job.ID, JobTypePipeline, "pipeline", fmt.Sprintf("Stage %s failed after all retry attempts", stage), resultQueue)
			return
		}

		// Wait for stage completion signal
		stageTimeout := w.getStageTimeout(stage, job.Timeouts)
		select {
		case completedStage := <-job.StageSync:
			if completedStage != stage {
				w.sendErrorResult(job.ID, JobTypePipeline, "pipeline", fmt.Sprintf("Stage completion mismatch: expected %s, got %s", stage, completedStage), resultQueue)
				return
			}
			log.Printf("Pipeline job %s completed stage %s", job.ID, stage)
		case <-time.After(stageTimeout):
			w.sendErrorResult(job.ID, JobTypePipeline, "pipeline", fmt.Sprintf("Stage %s timed out after %v", stage, stageTimeout), resultQueue)
			return
		}
	}

	// All stages completed successfully
	job.Completed = true
	log.Printf("Pipeline job %s completed all %d stages successfully", job.ID, len(job.Stages))

	// Send final pipeline completion result
	resultQueue <- WorkerResult{
		JobID:   job.ID,
		JobType: JobTypePipeline,
		GovID:   "pipeline-complete",
		Result: map[string]interface{}{
			"completed_stages": job.Stages,
			"stage_results":    job.Results,
			"total_stages":     len(job.Stages),
		},
		Success:   true,
		Timestamp: time.Now(),
	}
}

// executeScrapeStage executes the scraping stage of the pipeline
func (w *Worker) executeScrapeStage(job *PipelineJob, resultQueue chan<- WorkerResult) {
	log.Printf("Executing scrape stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.ScrapeTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Create unique output directory for this pipeline stage
	outputDir := filepath.Join(w.WorkDir, fmt.Sprintf("pipeline-%s-scrape-%d", job.ID, time.Now().Unix()))

	// Configure container with pipeline-specific settings
	config := ContainerConfig{
		Image:     w.ScraperImage,
		GovIDs:    job.GovIDs,
		Mode:      "full", // Default to full scraping for pipelines
		OutputDir: outputDir,
		Environment: []string{
			fmt.Sprintf("JOB_ID=%s", job.ID),
			fmt.Sprintf("WORKER_ID=%s", w.ID),
			fmt.Sprintf("PIPELINE_STAGE=%s", PipelineStageScrape),
		},
	}

	// Override mode if specified in stage config
	if modeStr, ok := job.StageConfig["scrape_mode"].(string); ok {
		config.Mode = modeStr
	}

	// Create and start container
	containerID, err := CreateScraperContainer(config)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "scrape", fmt.Sprintf("Failed to create scrape container: %v", err), resultQueue)
		return
	}
	defer CleanupContainer(w.Client, containerID, outputDir)

	// Wait for container to complete with configurable timeout
	containerTimeout := time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	exitCode, containerLogs, err := WaitForContainerWithLogs(w.Client, containerID, containerTimeout)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "scrape", fmt.Sprintf("Container execution failed: %v", err), resultQueue)
		return
	}

	if exitCode != 0 {
		w.sendErrorResultWithLogs(job.ID, JobTypePipeline, "scrape", fmt.Sprintf("Container exited with code %d", exitCode), containerLogs, resultQueue)
		return
	}

	// Collect results from output directory
	results, err := CollectContainerResults(outputDir)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "scrape", fmt.Sprintf("Failed to collect results: %v", err), resultQueue)
		return
	}

	// Store scrape results in pipeline job
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageScrape] = results

	// Send success result for this stage
	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "scrape",
		Result:    results,
		Success:   true,
		Stage:     PipelineStageScrape,
		Timestamp: time.Now(),
		Logs:      containerLogs,
	}

	log.Printf("Worker %s completed scrape stage for pipeline job %s", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageScrape:
			// Stage completion signaled
		default:
			// Channel might be full or closed, log but continue
			log.Printf("Warning: Could not signal completion for scrape stage of job %s", job.ID)
		}
	}
}

// executeUploadStage executes the upload stage of the pipeline
func (w *Worker) executeUploadStage(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Executing upload stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.UploadTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get results from scrape stage
	scrapeResults, exists := job.Results[PipelineStageScrape]
	if !exists {
		w.sendErrorResult(job.ID, JobTypePipeline, "upload", "No scrape results available for upload", resultQueue)
		return
	}

	// Validate scrape results format
	if scrapeResults == nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "upload", "Scrape results are nil", resultQueue)
		return
	}

	log.Printf("Uploading scrape results to Dokito backend for job %s", job.ID)

	// Upload results to dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.UploadTimeoutMinutes)*time.Minute)
	defer cancel()

	uploadResult, err := w.uploadToDokitoWithContext(ctx, scrapeResults, dokitoBaseURL, timeouts)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "upload", fmt.Sprintf("Upload failed: %v", err), resultQueue)
		return
	}

	// Store upload results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageUpload] = uploadResult

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "upload",
		Result:    uploadResult,
		Success:   true,
		Stage:     PipelineStageUpload,
		Timestamp: time.Now(),
	}

	log.Printf("Worker %s completed upload stage for pipeline job %s", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageUpload:
			// Stage completion signaled
		default:
			log.Printf("Warning: Could not signal completion for upload stage of job %s", job.ID)
		}
	}
}

// executeProcessStage executes the processing stage of the pipeline
func (w *Worker) executeProcessStage(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Executing process stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.ProcessTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get upload results
	uploadResults, exists := job.Results[PipelineStageUpload]
	if !exists {
		w.sendErrorResult(job.ID, JobTypePipeline, "process", "No upload results available for processing", resultQueue)
		return
	}

	// Validate upload results format
	if uploadResults == nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "process", "Upload results are nil", resultQueue)
		return
	}

	log.Printf("Triggering processing in Dokito backend for job %s", job.ID)

	// Trigger processing in dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.ProcessTimeoutMinutes)*time.Minute)
	defer cancel()

	processResult, err := w.triggerDokitoProcessingWithContext(ctx, job.GetGovIDs(), dokitoBaseURL, timeouts)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "process", fmt.Sprintf("Processing failed: %v", err), resultQueue)
		return
	}

	// Store process results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageProcess] = processResult

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "process",
		Result:    processResult,
		Success:   true,
		Stage:     PipelineStageProcess,
		Timestamp: time.Now(),
	}

	log.Printf("Worker %s completed process stage for pipeline job %s", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageProcess:
			// Stage completion signaled
		default:
			log.Printf("Warning: Could not signal completion for process stage of job %s", job.ID)
		}
	}
}

// executeIngestStage executes the ingestion stage of the pipeline
func (w *Worker) executeIngestStage(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Executing ingest stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.IngestTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get process results
	processResults, exists := job.Results[PipelineStageProcess]
	if !exists {
		w.sendErrorResult(job.ID, JobTypePipeline, "ingest", "No process results available for ingestion", resultQueue)
		return
	}

	// Validate process results format
	if processResults == nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "ingest", "Process results are nil", resultQueue)
		return
	}

	log.Printf("Triggering ingestion in Dokito backend for job %s", job.ID)

	// Use the govIDs provided with the job
	govIDs := job.GetGovIDs()
	if len(govIDs) == 0 {
		w.sendErrorResult(job.ID, JobTypePipeline, "ingest", "No govIDs available for ingestion", resultQueue)
		return
	}

	// Trigger ingestion in dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.IngestTimeoutMinutes)*time.Minute)
	defer cancel()

	ingestResult, err := w.triggerDokitoIngestionWithContext(ctx, govIDs, dokitoBaseURL, timeouts)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypePipeline, "ingest", fmt.Sprintf("Ingestion failed: %v", err), resultQueue)
		return
	}

	// Store ingest results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageIngest] = ingestResult

	// Mark pipeline as completed
	job.Completed = true

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "ingest",
		Result:    ingestResult,
		Success:   true,
		Stage:     PipelineStageIngest,
		Timestamp: time.Now(),
	}

	log.Printf("Pipeline job %s completed all stages successfully", job.ID)

	// Signal final stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageIngest:
			// Final stage completion signaled
		case <-time.After(1 * time.Second):
			// Timeout signaling completion, but job is done anyway
		}
		close(job.StageSync) // Close the sync channel as pipeline is complete
	}
}

// executeScrapeStageWithRetry executes the scrape stage using ephemeral container
func (w *Worker) executeScrapeStageWithRetry(job *PipelineJob, resultQueue chan<- WorkerResult) error {
	log.Printf("Executing scrape stage for pipeline job %s using ephemeral container", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.ScrapeTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Create unique output directory for this pipeline stage
	outputDir := filepath.Join(w.WorkDir, fmt.Sprintf("pipeline-%s-scrape-%d", job.ID, time.Now().Unix()))

	// Configure container with pipeline-specific settings
	config := ContainerConfig{
		Image:     w.ScraperImage,
		GovIDs:    job.GovIDs,
		Mode:      "full", // Default to full scraping for pipelines
		OutputDir: outputDir,
		Environment: []string{
			fmt.Sprintf("JOB_ID=%s", job.ID),
			fmt.Sprintf("WORKER_ID=%s", w.ID),
			fmt.Sprintf("PIPELINE_STAGE=%s", PipelineStageScrape),
		},
	}

	// Override mode if specified in stage config
	if modeStr, ok := job.StageConfig["scrape_mode"].(string); ok {
		config.Mode = modeStr
	}

	// Create and start container
	containerID, err := CreateScraperContainer(config)
	if err != nil {
		return fmt.Errorf("failed to create scrape container: %w", err)
	}
	defer CleanupContainer(w.Client, containerID, outputDir)

	// Wait for container to complete with configurable timeout
	containerTimeout := time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	exitCode, containerLogs, err := WaitForContainerWithLogs(w.Client, containerID, containerTimeout)
	if err != nil {
		return fmt.Errorf("container execution failed: %w", err)
	}

	if exitCode != 0 {
		return fmt.Errorf("container exited with code %d: %s", exitCode, containerLogs)
	}

	// Collect results from output directory
	results, err := CollectContainerResults(outputDir)
	if err != nil {
		return fmt.Errorf("failed to collect results: %w", err)
	}

	// Store scrape results in pipeline job
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageScrape] = results

	// Send success result for this stage
	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "scrape",
		Result:    results,
		Success:   true,
		Stage:     PipelineStageScrape,
		Timestamp: time.Now(),
		Logs:      containerLogs,
	}

	log.Printf("✅ Worker %s completed scrape stage for pipeline job %s using ephemeral container", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageScrape:
			// Stage completion signaled
		default:
			// Channel might be full or closed, log but continue
			log.Printf("Warning: Could not signal completion for scrape stage of job %s", job.ID)
		}
	}

	return nil
}

// executeUploadStageWithRetry executes the upload stage and returns error for retry handling
func (w *Worker) executeUploadStageWithRetry(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) error {
	log.Printf("Executing upload stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.UploadTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get results from scrape stage
	scrapeResults, exists := job.Results[PipelineStageScrape]
	if !exists {
		return fmt.Errorf("no scrape results available for upload")
	}

	// Validate scrape results format
	if scrapeResults == nil {
		return fmt.Errorf("scrape results are nil")
	}

	log.Printf("Uploading scrape results to Dokito backend for job %s", job.ID)

	// Upload results to dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.UploadTimeoutMinutes)*time.Minute)
	defer cancel()

	uploadResult, err := w.uploadToDokitoWithContext(ctx, scrapeResults, dokitoBaseURL, timeouts)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	// Store upload results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageUpload] = uploadResult

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "upload",
		Result:    uploadResult,
		Success:   true,
		Stage:     PipelineStageUpload,
		Timestamp: time.Now(),
	}

	log.Printf("Worker %s completed upload stage for pipeline job %s", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageUpload:
			// Stage completion signaled
		default:
			log.Printf("Warning: Could not signal completion for upload stage of job %s", job.ID)
		}
	}

	return nil
}

// executeProcessStageWithRetry executes the process stage and returns error for retry handling
func (w *Worker) executeProcessStageWithRetry(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) error {
	log.Printf("Executing process stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.ProcessTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get upload results
	uploadResults, exists := job.Results[PipelineStageUpload]
	if !exists {
		return fmt.Errorf("no upload results available for processing")
	}

	// Validate upload results format
	if uploadResults == nil {
		return fmt.Errorf("upload results are nil")
	}

	log.Printf("Triggering processing in Dokito backend for job %s", job.ID)

	// Trigger processing in dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.ProcessTimeoutMinutes)*time.Minute)
	defer cancel()

	processResult, err := w.triggerDokitoProcessingWithContext(ctx, job.GetGovIDs(), dokitoBaseURL, timeouts)
	if err != nil {
		return fmt.Errorf("processing failed: %w", err)
	}

	// Store process results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageProcess] = processResult

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "process",
		Result:    processResult,
		Success:   true,
		Stage:     PipelineStageProcess,
		Timestamp: time.Now(),
	}

	log.Printf("Worker %s completed process stage for pipeline job %s", w.ID, job.ID)

	// Signal stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageProcess:
			// Stage completion signaled
		default:
			log.Printf("Warning: Could not signal completion for process stage of job %s", job.ID)
		}
	}

	return nil
}

// executeIngestStageWithRetry executes the ingest stage and returns error for retry handling
func (w *Worker) executeIngestStageWithRetry(job *PipelineJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) error {
	log.Printf("Executing ingest stage for pipeline job %s", job.ID)

	// Get timeout for this stage
	timeouts := job.Timeouts
	if timeouts.IngestTimeoutMinutes == 0 {
		timeouts = GetDefaultTimeouts()
	}

	// Get process results
	processResults, exists := job.Results[PipelineStageProcess]
	if !exists {
		return fmt.Errorf("no process results available for ingestion")
	}

	// Validate process results format
	if processResults == nil {
		return fmt.Errorf("process results are nil")
	}

	log.Printf("Triggering ingestion in Dokito backend for job %s", job.ID)

	// Use the govIDs provided with the job
	govIDs := job.GetGovIDs()
	if len(govIDs) == 0 {
		return fmt.Errorf("no govIDs available for ingestion")
	}

	// Trigger ingestion in dokito backend with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeouts.IngestTimeoutMinutes)*time.Minute)
	defer cancel()

	ingestResult, err := w.triggerDokitoIngestionWithContext(ctx, govIDs, dokitoBaseURL, timeouts)
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	// Store ingest results
	if job.Results == nil {
		job.Results = make(map[PipelineStage]interface{})
	}
	job.Results[PipelineStageIngest] = ingestResult

	// Mark pipeline as completed
	job.Completed = true

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypePipeline,
		GovID:     "ingest",
		Result:    ingestResult,
		Success:   true,
		Stage:     PipelineStageIngest,
		Timestamp: time.Now(),
	}

	log.Printf("Pipeline job %s completed all stages successfully", job.ID)

	// Signal final stage completion
	if job.StageSync != nil {
		select {
		case job.StageSync <- PipelineStageIngest:
			// Final stage completion signaled
		case <-time.After(1 * time.Second):
			// Timeout signaling completion, but job is done anyway
		}
		close(job.StageSync) // Close the sync channel as pipeline is complete
	}

	return nil
}

// hasNextStage checks if there are more stages after the current one
func (w *Worker) hasNextStage(job *PipelineJob, currentStage PipelineStage) bool {
	for i, stage := range job.Stages {
		if stage == currentStage && i < len(job.Stages)-1 {
			return true
		}
	}
	return false
}

// calculateBackoffDelay calculates the delay for retry attempts
func (w *Worker) calculateBackoffDelay(attempt int, retryConfig RetryConfig) time.Duration {
	delay := time.Duration(float64(retryConfig.InitialDelay) * math.Pow(retryConfig.BackoffFactor, float64(attempt-1)))
	if delay > retryConfig.MaxDelay {
		delay = retryConfig.MaxDelay
	}
	return delay
}

// isRetryableError checks if an error is retryable based on the retry configuration
func (w *Worker) isRetryableError(err error, retryConfig RetryConfig) bool {
	if err == nil {
		return false
	}

	errorMsg := strings.ToLower(err.Error())
	for _, retryableError := range retryConfig.RetryableErrors {
		if strings.Contains(errorMsg, strings.ToLower(retryableError)) {
			return true
		}
	}
	return false
}

// getStageTimeout returns the appropriate timeout for a pipeline stage
func (w *Worker) getStageTimeout(stage PipelineStage, timeouts TimeoutConfig) time.Duration {
	switch stage {
	case PipelineStageScrape:
		return time.Duration(timeouts.ScrapeTimeoutMinutes) * time.Minute
	case PipelineStageUpload:
		return time.Duration(timeouts.UploadTimeoutMinutes) * time.Minute
	case PipelineStageProcess:
		return time.Duration(timeouts.ProcessTimeoutMinutes) * time.Minute
	case PipelineStageIngest:
		return time.Duration(timeouts.IngestTimeoutMinutes) * time.Minute
	default:
		return 10 * time.Minute // Default timeout
	}
}

// processDokitoJob processes Dokito backend API jobs
func (w *Worker) processDokitoJob(job *DokitoJob, resultQueue chan<- WorkerResult, dokitoBaseURL string) {
	log.Printf("Worker %s processing dokito job %s with mode %s", w.ID, job.ID, job.Mode)

	var result interface{}
	var err error

	switch job.Mode {
	case DokitoModeCaseFetch:
		result, err = w.dokitoAPICall("GET", dokitoBaseURL+"/api/case", map[string]interface{}{
			"state":             job.State,
			"jurisdiction_name": job.JurisdictionName,
			"case_name":         job.CaseName,
		})
	case DokitoModeCaseList:
		result, err = w.dokitoAPICall("GET", dokitoBaseURL+"/api/cases", map[string]interface{}{
			"state":             job.State,
			"jurisdiction_name": job.JurisdictionName,
			"limit":             job.Limit,
			"offset":            job.Offset,
		})
	case DokitoModeAttachmentObj:
		result, err = w.dokitoAPICall("GET", dokitoBaseURL+"/api/attachment/"+job.Blake2bHash, nil)
	case DokitoModeAttachmentRaw:
		result, err = w.dokitoAPICall("GET", dokitoBaseURL+"/api/attachment/"+job.Blake2bHash+"/raw", nil)
	case DokitoModeCaseSubmit:
		result, err = w.dokitoAPICall("POST", dokitoBaseURL+"/api/case", job.CaseData)
	case DokitoModeReprocess:
		result, err = w.dokitoAPICall("POST", dokitoBaseURL+"/api/reprocess", map[string]interface{}{
			"operation_type":    job.OperationType,
			"state":             job.State,
			"jurisdiction_name": job.JurisdictionName,
		})
	default:
		err = fmt.Errorf("unsupported dokito job mode: %s", job.Mode)
	}

	if err != nil {
		w.sendErrorResult(job.ID, JobTypeDokito, "dokito", err.Error(), resultQueue)
		return
	}

	resultQueue <- WorkerResult{
		JobID:     job.ID,
		JobType:   JobTypeDokito,
		GovID:     "dokito",
		Result:    result,
		Success:   true,
		Timestamp: time.Now(),
	}

	log.Printf("Worker %s completed dokito job %s", w.ID, job.ID)
}

// processCaseListJobWithPersistentContainer executes a caselist job using docker exec
func (w *Worker) processCaseListJobWithPersistentContainer(job *CaseListJob, resultQueue chan<- WorkerResult) {
	log.Printf("🚀 Worker %s: Using persistent container %s for caselist job %s", w.ID, w.ContainerID[:12], job.ID)

	// Create job-specific subdirectory
	jobOutputSubdir := fmt.Sprintf("job-%s-%d", job.ID, time.Now().Unix())
	hostJobOutputDir := filepath.Join(w.WorkDir, "output", jobOutputSubdir)
	containerJobOutputPath := filepath.Join("/app/output", jobOutputSubdir)

	if err := os.MkdirAll(hostJobOutputDir, 0755); err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "setup", fmt.Sprintf("Failed to create output directory: %v", err), resultQueue)
		return
	}

	// Build environment variables for caselist mode
	envVars := []string{
		"GOVIDS=", // Empty for caselist
		"MODE=case-list",
		fmt.Sprintf("OUTPUT_DIR=%s", containerJobOutputPath),
		fmt.Sprintf("JOB_ID=%s", job.ID),
		fmt.Sprintf("WORKER_ID=%s", w.ID),
	}

	// Create exec config
	execConfig := types.ExecConfig{
		Cmd:          []string{"node", "scraper-container.js"},
		Env:          envVars,
		WorkingDir:   "/app",
		AttachStdout: true,
		AttachStderr: true,
	}

	// Create and start exec
	execID, err := w.Client.ContainerExecCreate(context.Background(), w.ContainerID, execConfig)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "exec", fmt.Sprintf("Failed to create exec: %v", err), resultQueue)
		return
	}

	log.Printf("📋 Worker %s: Starting exec in container %s for caselist job %s", w.ID, w.ContainerID[:12], job.ID)

	resp, err := w.Client.ContainerExecAttach(context.Background(), execID.ID, types.ExecStartCheck{})
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "exec", fmt.Sprintf("Failed to start exec: %v", err), resultQueue)
		return
	}
	defer resp.Close()

	log.Printf("⚡ Worker %s: Exec started, reading output...", w.ID)

	// Read logs with real-time logging
	var logBuffer bytes.Buffer
	logWriter := io.MultiWriter(&logBuffer, &StreamLogger{workerID: w.ID, jobID: job.ID})
	_, err = stdcopy.StdCopy(logWriter, logWriter, resp.Reader)
	if err != nil && err != io.EOF {
		log.Printf("Warning: Error reading exec output: %v", err)
	}
	containerLogs := logBuffer.String()

	log.Printf("📝 Worker %s: Exec output captured (%d bytes)", w.ID, len(containerLogs))

	// Wait for completion
	containerTimeout := 10 * time.Minute
	if timeouts := GetDefaultTimeouts(); timeouts.ContainerTimeoutMinutes > 0 {
		containerTimeout = time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	var exitCode int
	execCompleted := false

	for {
		select {
		case <-timeoutCtx.Done():
			w.sendErrorResult(job.ID, JobTypeCaseList, "timeout", "Caselist exec timed out", resultQueue)
			return
		case <-ticker.C:
			inspect, err := w.Client.ContainerExecInspect(context.Background(), execID.ID)
			if err != nil {
				w.sendErrorResult(job.ID, JobTypeCaseList, "inspect", fmt.Sprintf("Failed to inspect exec: %v", err), resultQueue)
				return
			}
			if !inspect.Running {
				exitCode = inspect.ExitCode
				execCompleted = true
				break
			}
		}
		if execCompleted {
			break
		}
	}

	if exitCode != 0 {
		w.sendErrorResultWithLogs(job.ID, JobTypeCaseList, "container", fmt.Sprintf("Caselist exec exited with code %d", exitCode), containerLogs, resultQueue)
		// Don't return - collect partial results if any
	}

	// Collect results
	results, err := CollectContainerResults(hostJobOutputDir)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "results", fmt.Sprintf("Failed to collect results: %v", err), resultQueue)
		return
	}

	// Get the result
	if genericResult, hasGeneric := results["result"]; hasGeneric {
		resultQueue <- WorkerResult{
			JobID:     job.ID,
			JobType:   JobTypeCaseList,
			GovID:     "caselist",
			Result:    genericResult,
			Success:   exitCode == 0,
			Timestamp: time.Now(),
			Logs:      containerLogs,
		}
	} else {
		w.sendErrorResult(job.ID, JobTypeCaseList, "results", "No caselist result found", resultQueue)
		return
	}

	log.Printf("✅ Worker %s completed caselist job %s using persistent container", w.ID, job.ID)
}

// processCaseListJob processes caselist fetching jobs using ephemeral containers
func (w *Worker) processCaseListJob(job *CaseListJob, resultQueue chan<- WorkerResult) {
	log.Printf("Worker %s processing caselist job %s", w.ID, job.ID)

	// Create unique output directory for this job
	outputDir := filepath.Join(w.WorkDir, fmt.Sprintf("job-%s-%d", job.ID, time.Now().Unix()))

	// Configure container for case-list mode
	config := ContainerConfig{
		Image:     w.ScraperImage,
		GovIDs:    []string{}, // Empty for case-list mode
		Mode:      "case-list",
		OutputDir: outputDir,
		Environment: []string{
			fmt.Sprintf("JOB_ID=%s", job.ID),
			fmt.Sprintf("WORKER_ID=%s", w.ID),
		},
	}

	// Create and start container
	containerID, err := CreateScraperContainer(config)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "container", fmt.Sprintf("Failed to create container: %v", err), resultQueue)
		return
	}
	defer CleanupContainer(w.Client, containerID, outputDir)

	// Wait for container to complete and capture logs with configurable timeout
	containerTimeout := 10 * time.Minute // Default timeout
	if timeouts := GetDefaultTimeouts(); timeouts.ContainerTimeoutMinutes > 0 {
		containerTimeout = time.Duration(timeouts.ContainerTimeoutMinutes) * time.Minute
	}
	exitCode, containerLogs, err := WaitForContainerWithLogs(w.Client, containerID, containerTimeout)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "container", fmt.Sprintf("Container execution failed: %v", err), resultQueue)
		return
	}

	if exitCode != 0 {
		w.sendErrorResultWithLogs(job.ID, JobTypeCaseList, "container", fmt.Sprintf("Container exited with code %d", exitCode), containerLogs, resultQueue)
		return
	}

	// Collect results from output directory
	results, err := CollectContainerResults(outputDir)
	if err != nil {
		w.sendErrorResult(job.ID, JobTypeCaseList, "results", fmt.Sprintf("Failed to collect results: %v", err), resultQueue)
		return
	}

	// Get the result array
	if genericResult, hasGeneric := results["result"]; hasGeneric {
		resultQueue <- WorkerResult{
			JobID:     job.ID,
			JobType:   JobTypeCaseList,
			GovID:     "caselist",
			Result:    genericResult,
			Success:   true,
			Timestamp: time.Now(),
			Logs:      containerLogs,
		}
	} else {
		w.sendErrorResult(job.ID, JobTypeCaseList, "results", "No caselist result found", resultQueue)
		return
	}

	log.Printf("Worker %s completed caselist job %s", w.ID, job.ID)
}

// dokitoAPICall makes HTTP requests to the Dokito backend API
func (w *Worker) dokitoAPICall(method, url string, data interface{}) (interface{}, error) {
	defaultTimeouts := GetDefaultTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeouts.HTTPTimeoutSeconds)*time.Second)
	defer cancel()
	return w.dokitoAPICallWithContext(ctx, method, url, data, defaultTimeouts)
}

// dokitoAPICallWithContext makes HTTP requests to the Dokito backend API with context
func (w *Worker) dokitoAPICallWithContext(ctx context.Context, method, url string, data interface{}, timeouts TimeoutConfig) (interface{}, error) {
	var reqBody io.Reader

	if data != nil {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request data: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", fmt.Sprintf("JobRunner-Worker-%s", w.ID))

	// Use configurable timeout
	httpTimeout := time.Duration(timeouts.HTTPTimeoutSeconds) * time.Second
	client := &http.Client{Timeout: httpTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result, nil
}

// convertToRawDockets converts scrape results to Dokito RawGenericDocket format
func (w *Worker) convertToRawDockets(scrapeResults interface{}) ([]map[string]interface{}, error) {
	var rawDockets []map[string]interface{}

	// Handle different scrape result formats
	switch results := scrapeResults.(type) {
	case map[string]interface{}:
		// Single result or map of results by govID
		for govID, resultData := range results {
			if govID == "summary" || govID == "metadata" {
				continue // Skip non-docket data
			}

			docket, err := w.convertSingleResultToRawDocket(govID, resultData)
			if err != nil {
				log.Printf("Warning: Failed to convert result for govID %s: %v", govID, err)
				continue
			}
			rawDockets = append(rawDockets, docket)
		}
	case []interface{}:
		// Array of results
		for i, resultData := range results {
			// Try to extract govID from the result data
			govID := fmt.Sprintf("unknown-%d", i)
			if resultMap, ok := resultData.(map[string]interface{}); ok {
				if caseGovID, hasGovID := resultMap["case_govid"]; hasGovID {
					if govIDStr, ok := caseGovID.(string); ok {
						govID = govIDStr
					}
				}
			}

			docket, err := w.convertSingleResultToRawDocket(govID, resultData)
			if err != nil {
				log.Printf("Warning: Failed to convert result %d: %v", i, err)
				continue
			}
			rawDockets = append(rawDockets, docket)
		}
	default:
		return nil, fmt.Errorf("unsupported scrape results format: %T", scrapeResults)
	}

	if len(rawDockets) == 0 {
		return nil, fmt.Errorf("no valid dockets could be converted from scrape results")
	}

	return rawDockets, nil
}

// convertSingleResultToRawDocket converts a single scrape result to RawGenericDocket format
func (w *Worker) convertSingleResultToRawDocket(govID string, resultData interface{}) (map[string]interface{}, error) {
	resultMap, ok := resultData.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("result data is not a map: %T", resultData)
	}

	// Extract case govID
	caseGovID := govID
	if extractedGovID, hasGovID := resultMap["case_govid"]; hasGovID {
		if govIDStr, ok := extractedGovID.(string); ok {
			caseGovID = govIDStr
		}
	}

	// Create RawGenericDocket structure according to Dokito types
	rawDocket := map[string]interface{}{
		"case_govid":        caseGovID,
		"state":             "ny", // Assuming NY PUC for now
		"jurisdiction_name": "ny_puc",
		"scraper_metadata": map[string]interface{}{
			"worker_id": w.ID,
			"timestamp": time.Now().Format(time.RFC3339),
			"source":    "jobrunner",
		},
	}

	// Add all other fields from the scrape result
	for key, value := range resultMap {
		if key != "case_govid" { // Don't duplicate case_govid
			rawDocket[key] = value
		}
	}

	return rawDocket, nil
}

// extractGovIDsFromScrapeResults extracts govIDs from original scrape results as fallback
func (w *Worker) extractGovIDsFromScrapeResults(scrapeResults interface{}) ([]string, error) {
	var govIDs []string

	switch res := scrapeResults.(type) {
	case map[string]interface{}:
		// Try to extract from top-level keys that look like govIDs
		for key := range res {
			// NY PUC govID pattern: XX-XXXXX
			if matched, _ := regexp.MatchString(`^\d{2}-\d{5}$`, key); matched {
				govIDs = append(govIDs, key)
			}
		}

		// Also check if there's a summary with gov_ids
		if summary, hasSummary := res["summary"]; hasSummary {
			if summaryMap, ok := summary.(map[string]interface{}); ok {
				if ids, hasIDs := summaryMap["gov_ids"]; hasIDs {
					if idArray, ok := ids.([]interface{}); ok {
						for _, id := range idArray {
							if idStr, ok := id.(string); ok {
								govIDs = append(govIDs, idStr)
							}
						}
					}
				}
			}
		}
	case []interface{}:
		// Array format - extract case_govid from each item
		for _, item := range res {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if govID, hasGovID := itemMap["case_govid"]; hasGovID {
					if govIDStr, ok := govID.(string); ok {
						govIDs = append(govIDs, govIDStr)
					}
				}
			}
		}
	default:
		return nil, fmt.Errorf("unsupported scrape results format for govID extraction: %T", scrapeResults)
	}

	if len(govIDs) == 0 {
		return nil, fmt.Errorf("no govIDs could be extracted from scrape results")
	}

	// Remove duplicates
	govIDSet := make(map[string]bool)
	var uniqueGovIDs []string
	for _, govID := range govIDs {
		if !govIDSet[govID] {
			govIDSet[govID] = true
			uniqueGovIDs = append(uniqueGovIDs, govID)
		}
	}

	return uniqueGovIDs, nil
}

// uploadToDokito uploads scraped data to the Dokito backend
func (w *Worker) uploadToDokito(scrapeResults interface{}, dokitoBaseURL string) (interface{}, error) {
	defaultTimeouts := GetDefaultTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeouts.HTTPTimeoutSeconds)*time.Second)
	defer cancel()
	return w.uploadToDokitoWithContext(ctx, scrapeResults, dokitoBaseURL, defaultTimeouts)
}

// uploadToDokitoWithContext uploads scraped data to the Dokito backend with context
func (w *Worker) uploadToDokitoWithContext(ctx context.Context, scrapeResults interface{}, dokitoBaseURL string, timeouts TimeoutConfig) (interface{}, error) {
	log.Printf("Worker %s uploading scrape results to Dokito backend", w.ID)

	// Convert scrape results to Dokito RawGenericDocket format
	rawDockets, err := w.convertToRawDockets(scrapeResults)
	if err != nil {
		return nil, fmt.Errorf("failed to convert scrape results to raw dockets: %w", err)
	}

	uploadData := map[string]interface{}{
		"action":  "upload_raw",
		"dockets": rawDockets,
	}

	// Use the correct Dokito endpoint for uploading raw dockets
	// Assuming NY PUC for now - this should be configurable
	url := fmt.Sprintf("%s/admin/docket-process/ny/ny_puc/raw-dockets", dokitoBaseURL)

	uploadResult, err := w.dokitoAPICallWithContext(ctx, "POST", url, uploadData, timeouts)
	if err != nil {
		return nil, err
	}

	// Check the upload response for errors
	if err := w.validateUploadResponse(uploadResult); err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}

	return uploadResult, nil
}

// validateUploadResponse checks if the Dokito upload response indicates success
func (w *Worker) validateUploadResponse(uploadResult interface{}) error {
	resultMap, ok := uploadResult.(map[string]interface{})
	if !ok {
		return fmt.Errorf("upload response is not a valid map: %T", uploadResult)
	}

	// Check for error_count field
	if errorCount, hasErrorCount := resultMap["error_count"]; hasErrorCount {
		if errorCountNum, ok := errorCount.(float64); ok && errorCountNum > 0 {
			successCount := float64(0)
			if sc, hasSuccess := resultMap["success_count"]; hasSuccess {
				if scNum, ok := sc.(float64); ok {
					successCount = scNum
				}
			}
			return fmt.Errorf("dokito backend upload failed: %v errors, %v successes", errorCountNum, successCount)
		}
	}

	// Check for empty successfully_processed_dockets
	if processedDockets, hasProcessed := resultMap["successfully_processed_dockets"]; hasProcessed {
		if docketArray, ok := processedDockets.([]interface{}); ok && len(docketArray) == 0 {
			// Empty array might indicate failure, but check success_count to be sure
			if successCount, hasSuccess := resultMap["success_count"]; hasSuccess {
				if successCountNum, ok := successCount.(float64); ok && successCountNum == 0 {
					return fmt.Errorf("dokito backend upload failed: no dockets were successfully processed")
				}
			}
		}
	}

	log.Printf("Worker %s: Upload validation passed", w.ID)
	return nil
}

// triggerDokitoProcessing triggers processing of uploaded data in Dokito backend
func (w *Worker) triggerDokitoProcessing(govIDs []string, dokitoBaseURL string) (interface{}, error) {
	defaultTimeouts := GetDefaultTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeouts.HTTPTimeoutSeconds)*time.Second)
	defer cancel()

	return w.triggerDokitoProcessingWithContext(ctx, govIDs, dokitoBaseURL, defaultTimeouts)
}

// triggerDokitoProcessingWithContext triggers processing of uploaded data in Dokito backend with context
func (w *Worker) triggerDokitoProcessingWithContext(ctx context.Context, govIDs []string, dokitoBaseURL string, timeouts TimeoutConfig) (interface{}, error) {
	log.Printf("Worker %s triggering Dokito processing for govIDs: %v", w.ID, govIDs)

	processData := map[string]interface{}{
		"docket_ids": govIDs,
	}

	// Use the correct Dokito endpoint for processing by govID
	url := fmt.Sprintf("%s/admin/docket-process/ny/ny_puc/govid/process", dokitoBaseURL)
	return w.dokitoAPICallWithContext(ctx, "POST", url, processData, timeouts)
}

// triggerDokitoIngestion triggers ingestion of processed data in Dokito backend
func (w *Worker) triggerDokitoIngestion(govIDs []string, dokitoBaseURL string) (interface{}, error) {
	defaultTimeouts := GetDefaultTimeouts()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(defaultTimeouts.HTTPTimeoutSeconds)*time.Second)
	defer cancel()
	return w.triggerDokitoIngestionWithContext(ctx, govIDs, dokitoBaseURL, defaultTimeouts)
}

// triggerDokitoIngestionWithContext triggers ingestion of processed data in Dokito backend with context
func (w *Worker) triggerDokitoIngestionWithContext(ctx context.Context, govIDs []string, dokitoBaseURL string, timeouts TimeoutConfig) (interface{}, error) {
	log.Printf("Worker %s triggering Dokito ingestion for govIDs: %v", w.ID, govIDs)

	ingestData := map[string]interface{}{
		"docket_ids": govIDs,
	}

	// Use the correct Dokito endpoint for ingestion by govID
	url := fmt.Sprintf("%s/admin/docket-process/ny/ny_puc/govid/ingest", dokitoBaseURL)
	return w.dokitoAPICallWithContext(ctx, "POST", url, ingestData, timeouts)
}

// GovIDPartition represents a partition of gov IDs for map-reduce processing
type GovIDPartition struct {
	PartitionID int
	GovIDs      []string
	StartIndex  int
	EndIndex    int
}

// partitionGovIDs splits a slice of gov IDs into partitions for parallel processing
func (r *Runner) partitionGovIDs(govIDs []string, maxPartitionSize int) []GovIDPartition {
	if len(govIDs) == 0 {
		return []GovIDPartition{}
	}

	if maxPartitionSize <= 0 {
		maxPartitionSize = 6 // Default partition size
	}

	var partitions []GovIDPartition
	partitionID := 0

	for i := 0; i < len(govIDs); i += maxPartitionSize {
		end := i + maxPartitionSize
		if end > len(govIDs) {
			end = len(govIDs)
		}

		partition := GovIDPartition{
			PartitionID: partitionID,
			GovIDs:      govIDs[i:end],
			StartIndex:  i,
			EndIndex:    end,
		}

		partitions = append(partitions, partition)
		partitionID++
	}

	log.Printf("Partitioned %d govIDs into %d partitions (max size: %d)", len(govIDs), len(partitions), maxPartitionSize)
	return partitions
}

// MapReduceResult represents the result of a map-reduce operation
type MapReduceResult struct {
	JobID       string
	TotalGovIDs int
	Partitions  int
	Results     map[string]interface{}
	Errors      []string
	Success     bool
	StartTime   time.Time
	EndTime     time.Time
	Duration    time.Duration
}

// processGovIDsMapReduce processes gov IDs using map-reduce pattern
func (r *Runner) processGovIDsMapReduce(baseJob BaseJob, jobConfig interface{}, maxPartitionSize int) *MapReduceResult {
	startTime := time.Now()
	log.Printf("Starting map-reduce processing for job %s with %d govIDs", baseJob.ID, len(baseJob.GovIDs))

	// Partition the gov IDs
	partitions := r.partitionGovIDs(baseJob.GovIDs, maxPartitionSize)
	if len(partitions) == 0 {
		return &MapReduceResult{
			JobID:     baseJob.ID,
			Success:   false,
			Errors:    []string{"No govIDs to process"},
			StartTime: startTime,
			EndTime:   time.Now(),
		}
	}

	// Channel to collect partition results
	partitionResults := make(chan map[string]interface{}, len(partitions))
	partitionErrors := make(chan string, len(partitions))

	// Process each partition in parallel
	var wg sync.WaitGroup
	for _, partition := range partitions {
		wg.Add(1)
		go func(p GovIDPartition) {
			defer wg.Done()
			r.processPartition(baseJob, p, jobConfig, partitionResults, partitionErrors)
		}(partition)
	}

	// Wait for all partitions to complete
	wg.Wait()
	close(partitionResults)
	close(partitionErrors)

	// Reduce: aggregate results from all partitions
	combinedResults := make(map[string]interface{})
	var errors []string

	for result := range partitionResults {
		for k, v := range result {
			combinedResults[k] = v
		}
	}

	for err := range partitionErrors {
		errors = append(errors, err)
	}

	endTime := time.Now()
	result := &MapReduceResult{
		JobID:       baseJob.ID,
		TotalGovIDs: len(baseJob.GovIDs),
		Partitions:  len(partitions),
		Results:     combinedResults,
		Errors:      errors,
		Success:     len(errors) < len(partitions), // Success if at least some partitions succeeded
		StartTime:   startTime,
		EndTime:     endTime,
		Duration:    endTime.Sub(startTime),
	}

	log.Printf("Map-reduce processing completed for job %s: %d/%d partitions successful, duration: %v",
		baseJob.ID, len(partitions)-len(errors), len(partitions), result.Duration)

	return result
}

// processPartition processes a single partition of gov IDs
func (r *Runner) processPartition(baseJob BaseJob, partition GovIDPartition, jobConfig interface{},
	results chan<- map[string]interface{}, errors chan<- string,
) {
	log.Printf("Processing partition %d for job %s (%d govIDs: %v)",
		partition.PartitionID, baseJob.ID, len(partition.GovIDs), partition.GovIDs)

	// Create a partition-specific job
	partitionJobID := fmt.Sprintf("%s-partition-%d", baseJob.ID, partition.PartitionID)

	// Create job based on type
	var partitionJob WorkerJob
	switch baseJob.Type {
	case JobTypeScrape:
		if scrapeConfig, ok := jobConfig.(*ScrapeJob); ok {
			partitionJob = &ScrapeJob{
				BaseJob: BaseJob{
					ID:     partitionJobID,
					Type:   JobTypeScrape,
					GovIDs: partition.GovIDs,
				},
				Mode:       scrapeConfig.Mode,
				DateString: scrapeConfig.DateString,
				BeginDate:  scrapeConfig.BeginDate,
				EndDate:    scrapeConfig.EndDate,
			}
		}
	case JobTypeDokito:
		if dokitoConfig, ok := jobConfig.(*DokitoJob); ok {
			partitionJob = &DokitoJob{
				BaseJob: BaseJob{
					ID:     partitionJobID,
					Type:   JobTypeDokito,
					GovIDs: partition.GovIDs,
				},
				Mode:             dokitoConfig.Mode,
				State:            dokitoConfig.State,
				JurisdictionName: dokitoConfig.JurisdictionName,
				CaseName:         dokitoConfig.CaseName,
				Blake2bHash:      dokitoConfig.Blake2bHash,
				CaseData:         dokitoConfig.CaseData,
				OperationType:    dokitoConfig.OperationType,
				Limit:            dokitoConfig.Limit,
				Offset:           dokitoConfig.Offset,
			}
		}
	case JobTypePipeline:
		if pipelineConfig, ok := jobConfig.(*PipelineJob); ok {
			partitionJob = &PipelineJob{
				BaseJob: BaseJob{
					ID:     partitionJobID,
					Type:   JobTypePipeline,
					GovIDs: partition.GovIDs,
				},
				Stages:       pipelineConfig.Stages,
				CurrentStage: pipelineConfig.CurrentStage,
				StageConfig:  pipelineConfig.StageConfig,
				ParentJobID:  baseJob.ID, // Reference original job
				Results:      make(map[PipelineStage]interface{}),
			}
		}
	default:
		errors <- fmt.Sprintf("Unsupported job type for partition: %s", baseJob.Type)
		return
	}

	if partitionJob == nil {
		errors <- fmt.Sprintf("Failed to create partition job for partition %d", partition.PartitionID)
		return
	}

	// Submit partition job and collect results with configurable timeout
	partitionTimeout := 5 * time.Minute // Default timeout
	if pipelineJob, ok := jobConfig.(*PipelineJob); ok && pipelineJob.Timeouts.ScrapeTimeoutMinutes > 0 {
		// Use scrape timeout for scraping partitions
		partitionTimeout = time.Duration(pipelineJob.Timeouts.ScrapeTimeoutMinutes) * time.Minute
	}
	partitionResults := r.submitJobAndWait(partitionJob, partitionTimeout)
	if len(partitionResults) == 0 {
		errors <- fmt.Sprintf("No results received for partition %d", partition.PartitionID)
		return
	}

	results <- partitionResults
	log.Printf("Partition %d completed successfully with %d results", partition.PartitionID, len(partitionResults))
}

// submitJobAndWait submits a job and waits for all results
func (r *Runner) submitJobAndWait(job WorkerJob, timeout time.Duration) map[string]interface{} {
	resultsChan := make(chan WorkerResult, len(job.GetGovIDs())+1)
	expectedResults := len(job.GetGovIDs())
	if job.GetType() == JobTypePipeline {
		expectedResults = 1 // Pipeline jobs typically return one result per stage
	}

	// Temporarily redirect results to our channel
	originalQueue := r.resultQueue
	r.resultQueue = resultsChan

	// Submit the job
	r.submitJobToWorker(job)

	// Wait for results with timeout
	results := make(map[string]interface{})
	timeoutChan := time.After(timeout)
	receivedCount := 0

waitLoop:
	for {
		select {
		case result := <-resultsChan:
			if result.Success {
				key := result.GovID
				if key == "" {
					key = fmt.Sprintf("result_%d", receivedCount)
				}
				results[key] = result.Result
			}
			receivedCount++
			if receivedCount >= expectedResults {
				break waitLoop
			}
		case <-timeoutChan:
			log.Printf("Timeout waiting for job %s results (got %d/%d)", job.GetID(), receivedCount, expectedResults)
			break waitLoop
		case <-r.ctx.Done():
			break waitLoop
		}
	}

	// Restore original result queue
	r.resultQueue = originalQueue

	return results
}

// storeJobResult stores the job result and log files persistently
func (r *Runner) storeJobResult(result WorkerResult) error {
	// Create job-specific directory
	jobDir := filepath.Join(r.resultsDir, fmt.Sprintf("%s_%s_%s",
		result.JobID, result.GovID, result.Timestamp.Format("20060102_150405")))

	if err := os.MkdirAll(jobDir, 0755); err != nil {
		return fmt.Errorf("failed to create job directory: %w", err)
	}

	// Store result.json
	resultFile := filepath.Join(jobDir, "result.json")
	var resultData interface{}

	if result.Success {
		resultData = result.Result
	} else {
		resultData = map[string]interface{}{
			"error":     result.Error,
			"gov_id":    result.GovID,
			"job_id":    result.JobID,
			"job_type":  result.JobType,
			"timestamp": result.Timestamp,
			"success":   false,
		}
	}

	resultJSON, err := json.MarshalIndent(resultData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	if err := os.WriteFile(resultFile, resultJSON, 0644); err != nil {
		return fmt.Errorf("failed to write result file: %w", err)
	}

	// Store logfile with container stdout/stderr
	logFile := filepath.Join(jobDir, "logfile.txt")

	var logData string
	if result.Logs != "" {
		// Use the actual container logs
		logData = result.Logs
	} else {
		// Fallback to summary if no logs were captured
		logData = fmt.Sprintf(`Job Execution Log (No container logs captured)
==================
Job ID: %s
Gov ID: %s
Job Type: %s
Timestamp: %s
Success: %v
Duration: %v

`, result.JobID, result.GovID, result.JobType,
			result.Timestamp.Format("2006-01-02 15:04:05"), result.Success,
			time.Since(result.Timestamp))

		if result.Success {
			logData += fmt.Sprintf("Result: Successfully processed %s\n", result.GovID)
		} else {
			logData += fmt.Sprintf("Error: %s\n", result.Error)
		}
	}

	if err := os.WriteFile(logFile, []byte(logData), 0644); err != nil {
		return fmt.Errorf("failed to write log file: %w", err)
	}

	log.Printf("📁 Stored job files: %s", jobDir)
	return nil
}

// collectResults collects results from all workers
func (r *Runner) collectResults() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case result := <-r.resultQueue:
			r.resultsMux.Lock()
			key := fmt.Sprintf("%s_%s", result.JobID, result.GovID)
			if result.Success {
				r.results[key] = result.Result
			} else {
				r.results[key] = map[string]interface{}{
					"error":     result.Error,
					"gov_id":    result.GovID,
					"job_id":    result.JobID,
					"job_type":  result.JobType,
					"timestamp": result.Timestamp,
				}
			}
			r.resultsMux.Unlock()

			// Store result and log files persistently
			if err := r.storeJobResult(result); err != nil {
				log.Printf("Failed to store job result: %v", err)
			}

			// Update queue state if govID was being tracked
			if result.GovID != "" && r.queueState != nil {
				if result.Success {
					r.MarkGovIDCompleted(result.GovID)
					log.Printf("✅ GovID %s completed successfully", result.GovID)
				} else {
					r.MarkGovIDFailed(result.GovID, result.Error)
					log.Printf("❌ GovID %s failed: %s", result.GovID, result.Error)
				}
			}

			log.Printf("Collected result for govID %s from job %s (type: %s, success: %v)",
				result.GovID, result.JobID, result.JobType, result.Success)
		}
	}
}

// SubmitScrapeJob submits a scraping job to be processed by workers
func (r *Runner) SubmitScrapeJob(govIDs []string, mode ScrapingMode, useMapReduce bool, maxPartitionSize int) string {
	jobID := fmt.Sprintf("scrape-%d", time.Now().UnixNano())

	job := &ScrapeJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypeScrape,
			GovIDs: govIDs,
		},
		Mode: mode,
	}

	if useMapReduce && len(govIDs) > maxPartitionSize {
		log.Printf("Submitting scrape job %s with map-reduce (govIDs: %d, partition size: %d)", jobID, len(govIDs), maxPartitionSize)
		go func() {
			result := r.processGovIDsMapReduce(job.BaseJob, job, maxPartitionSize)
			log.Printf("Map-reduce scrape job %s completed: success=%v, duration=%v", jobID, result.Success, result.Duration)
		}()
	} else {
		r.submitJobToWorker(job)
	}

	return jobID
}

// SubmitScrapeJobWithDates submits a scraping job with date parameters
func (r *Runner) SubmitScrapeJobWithDates(govIDs []string, mode ScrapingMode, dateString, beginDate, endDate string) string {
	jobID := fmt.Sprintf("scrape-dates-%d", time.Now().UnixNano())

	job := &ScrapeJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypeScrape,
			GovIDs: govIDs,
		},
		Mode:       mode,
		DateString: dateString,
		BeginDate:  beginDate,
		EndDate:    endDate,
	}

	r.submitJobToWorker(job)
	return jobID
}

// SubmitCaseListJob submits a caselist fetching job
func (r *Runner) SubmitCaseListJob() string {
	jobID := fmt.Sprintf("caselist-%d", time.Now().UnixNano())

	job := &CaseListJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypeCaseList,
			GovIDs: []string{}, // Empty for caselist jobs
		},
	}

	r.submitJobToWorker(job)
	return jobID
}

// SubmitPipelineJob submits a pipeline job with multiple stages
func (r *Runner) SubmitPipelineJob(govIDs []string, stages []PipelineStage, stageConfig map[string]interface{}, useMapReduce bool, maxPartitionSize int) string {
	return r.SubmitPipelineJobWithTimeouts(govIDs, stages, stageConfig, GetDefaultTimeouts(), useMapReduce, maxPartitionSize)
}

// SubmitPipelineJobToWorker submits a pipeline job to a specific worker
func (r *Runner) SubmitPipelineJobToWorker(govIDs []string, stages []PipelineStage, stageConfig map[string]interface{}, useMapReduce bool, maxPartitionSize int, workerID string) string {
	return r.SubmitPipelineJobWithTimeoutsToWorker(govIDs, stages, stageConfig, GetDefaultTimeouts(), useMapReduce, maxPartitionSize, workerID)
}

// SubmitPipelineJobWithTimeouts submits a pipeline job with custom timeout configuration
func (r *Runner) SubmitPipelineJobWithTimeouts(govIDs []string, stages []PipelineStage, stageConfig map[string]interface{}, timeouts TimeoutConfig, useMapReduce bool, maxPartitionSize int) string {
	jobID := fmt.Sprintf("pipeline-%d", time.Now().UnixNano())

	if len(stages) == 0 {
		stages = []PipelineStage{PipelineStageScrape, PipelineStageUpload, PipelineStageProcess, PipelineStageIngest}
	}

	job := &PipelineJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypePipeline,
			GovIDs: govIDs,
		},
		Stages:       stages,
		CurrentStage: stages[0],
		StageConfig:  stageConfig,
		Results:      make(map[PipelineStage]interface{}),
		Timeouts:     timeouts,
		Completed:    false,
	}

	if useMapReduce && len(govIDs) > maxPartitionSize {
		log.Printf("Submitting pipeline job %s with map-reduce (govIDs: %d, partition size: %d)", jobID, len(govIDs), maxPartitionSize)
		go func() {
			result := r.processGovIDsMapReduce(job.BaseJob, job, maxPartitionSize)
			log.Printf("Map-reduce pipeline job %s completed: success=%v, duration=%v", jobID, result.Success, result.Duration)
		}()
	} else {
		log.Printf("Submitting sequential pipeline job %s with %d stages for %d govIDs", jobID, len(stages), len(govIDs))
		r.submitJobToWorker(job)
	}

	return jobID
}

// SubmitPipelineJobWithTimeoutsToWorker submits a pipeline job to a specific worker with custom timeout configuration
func (r *Runner) SubmitPipelineJobWithTimeoutsToWorker(govIDs []string, stages []PipelineStage, stageConfig map[string]interface{}, timeouts TimeoutConfig, useMapReduce bool, maxPartitionSize int, workerID string) string {
	jobID := fmt.Sprintf("pipeline-%d", time.Now().UnixNano())

	if len(stages) == 0 {
		stages = []PipelineStage{PipelineStageScrape, PipelineStageUpload, PipelineStageProcess, PipelineStageIngest}
	}

	job := &PipelineJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypePipeline,
			GovIDs: govIDs,
		},
		Stages:       stages,
		CurrentStage: stages[0],
		StageConfig:  stageConfig,
		Results:      make(map[PipelineStage]interface{}),
		Timeouts:     timeouts,
		Completed:    false,
	}

	if useMapReduce && len(govIDs) > maxPartitionSize {
		log.Printf("Submitting pipeline job %s with map-reduce (govIDs: %d, partition size: %d) to worker %s", jobID, len(govIDs), maxPartitionSize, workerID)
		// Note: Map-reduce currently doesn't support worker pinning, falling back to any worker
		go func() {
			result := r.processGovIDsMapReduce(job.BaseJob, job, maxPartitionSize)
			log.Printf("Map-reduce pipeline job %s completed: success=%v, duration=%v", jobID, result.Success, result.Duration)
		}()
	} else {
		log.Printf("Submitting sequential pipeline job %s with %d stages for %d govIDs to worker %s", jobID, len(stages), len(govIDs), workerID)
		r.submitJobToSpecificWorker(job, workerID)
	}

	return jobID
}

// SubmitDokitoJob submits a Dokito backend API job
func (r *Runner) SubmitDokitoJob(mode DokitoMode, state, jurisdictionName, caseName, hash string, caseData interface{}, operationType string, limit, offset int) string {
	jobID := fmt.Sprintf("dokito-%d", time.Now().UnixNano())

	job := &DokitoJob{
		BaseJob: BaseJob{
			ID:     jobID,
			Type:   JobTypeDokito,
			GovIDs: []string{}, // Dokito jobs don't typically use govIDs
		},
		Mode:             mode,
		State:            state,
		JurisdictionName: jurisdictionName,
		CaseName:         caseName,
		Blake2bHash:      hash,
		CaseData:         caseData,
		OperationType:    operationType,
		Limit:            limit,
		Offset:           offset,
	}

	r.submitJobToWorker(job)
	return jobID
}

// SubmitPipelineJobForGovID submits a complete pipeline job for a single govID
func (r *Runner) SubmitPipelineJobForGovID(govID string) string {
	return r.SubmitPipelineJobForGovIDs([]string{govID})
}

// SubmitPipelineJobForGovIDs submits a complete pipeline job for multiple govIDs
func (r *Runner) SubmitPipelineJobForGovIDs(govIDs []string) string {
	// Full pipeline: scrape -> upload -> process -> ingest
	stages := []PipelineStage{
		PipelineStageScrape,
		PipelineStageUpload,
		PipelineStageProcess,
		PipelineStageIngest,
	}

	// Default configuration for complete pipeline
	stageConfig := map[string]interface{}{
		"scrape_mode":   "all",
		"upload_format": "json",
		"process_type":  "standard",
		"validate_data": true,
	}

	// Use map-reduce for multiple govIDs
	useMapReduce := len(govIDs) > 6
	maxPartitionSize := 6

	return r.SubmitPipelineJob(govIDs, stages, stageConfig, useMapReduce, maxPartitionSize)
}

// SubmitPipelineJobForGovIDsToWorker submits a complete pipeline job for multiple govIDs to a specific worker
func (r *Runner) SubmitPipelineJobForGovIDsToWorker(govIDs []string, workerID string) string {
	// Full pipeline: scrape -> upload -> process -> ingest
	stages := []PipelineStage{
		PipelineStageScrape,
		PipelineStageUpload,
		PipelineStageProcess,
		PipelineStageIngest,
	}

	// Default configuration for complete pipeline
	stageConfig := map[string]interface{}{
		"scrape_mode":   "all",
		"upload_format": "json",
		"process_type":  "standard",
		"validate_data": true,
	}

	// Use map-reduce for multiple govIDs
	useMapReduce := len(govIDs) > 6
	maxPartitionSize := 6

	return r.SubmitPipelineJobToWorker(govIDs, stages, stageConfig, useMapReduce, maxPartitionSize, workerID)
}

// submitJobToWorker distributes job to available worker
// submitJobToWorker submits a job to the shared job queue
func (r *Runner) submitJobToWorker(job WorkerJob) {
	select {
	case r.jobQueue <- job:
		log.Printf("Submitted job %s (type: %s) to job queue", job.GetID(), job.GetType())
	default:
		// Queue is full, submit in background to avoid blocking
		go func() {
			r.jobQueue <- job
			log.Printf("Submitted job %s (type: %s) to job queue (queued)", job.GetID(), job.GetType())
		}()
	}
}

// getWorkerByID retrieves a worker by its ID
func (r *Runner) getWorkerByID(workerID string) *Worker {
	for _, worker := range r.workers {
		if worker.ID == workerID {
			return worker
		}
	}
	return nil
}

// submitJobToSpecificWorker submits a job to the shared queue
// NOTE: With a shared queue, we can't guarantee which worker picks up the job
// This method is kept for backwards compatibility but now just submits to the queue
func (r *Runner) submitJobToSpecificWorker(job WorkerJob, workerID string) {
	log.Printf("Submitting job %s to queue (requested worker: %s, but will be picked by next idle worker)", job.GetID(), workerID)
	r.submitJobToWorker(job)
}

// JobSummary provides a summary of job execution
type JobSummary struct {
	JobID       string                 `json:"job_id"`
	JobType     JobType                `json:"job_type"`
	TotalGovIDs int                    `json:"total_gov_ids"`
	Successful  int                    `json:"successful"`
	Failed      int                    `json:"failed"`
	Results     map[string]interface{} `json:"results"`
	Errors      []string               `json:"errors"`
	StartTime   *time.Time             `json:"start_time,omitempty"`
	EndTime     *time.Time             `json:"end_time,omitempty"`
	Duration    *time.Duration         `json:"duration,omitempty"`
}

// GetResults returns a copy of all collected results
func (r *Runner) GetResults() map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	results := make(map[string]interface{})
	for k, v := range r.results {
		results[k] = v
	}
	return results
}

// GetResult returns the result for a specific key
func (r *Runner) GetResult(key string) (interface{}, bool) {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	result, exists := r.results[key]
	return result, exists
}

// GetJobResults returns all results for a specific job ID
func (r *Runner) GetJobResults(jobID string) map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	jobResults := make(map[string]interface{})
	for key, result := range r.results {
		if strings.HasPrefix(key, jobID+"_") {
			govID := strings.TrimPrefix(key, jobID+"_")
			jobResults[govID] = result
		}
	}
	return jobResults
}

// GetJobSummary returns a summary of job execution
func (r *Runner) GetJobSummary(jobID string) *JobSummary {
	jobResults := r.GetJobResults(jobID)

	summary := &JobSummary{
		JobID:   jobID,
		Results: make(map[string]interface{}),
		Errors:  []string{},
	}

	for govID, result := range jobResults {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if errorMsg, hasError := resultMap["error"]; hasError {
				summary.Failed++
				if errStr, ok := errorMsg.(string); ok {
					summary.Errors = append(summary.Errors, fmt.Sprintf("%s: %s", govID, errStr))
				}
			} else {
				summary.Successful++
				summary.Results[govID] = result
			}

			// Extract job type if available
			if jobType, hasType := resultMap["job_type"]; hasType {
				if typeStr, ok := jobType.(string); ok {
					summary.JobType = JobType(typeStr)
				}
			}
		} else {
			summary.Successful++
			summary.Results[govID] = result
		}
	}

	summary.TotalGovIDs = summary.Successful + summary.Failed
	return summary
}

// GetAllJobSummaries returns summaries for all jobs
func (r *Runner) GetAllJobSummaries() map[string]*JobSummary {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	jobMap := make(map[string]*JobSummary)

	// Extract unique job IDs
	jobIDs := make(map[string]bool)
	for key := range r.results {
		parts := strings.SplitN(key, "_", 2)
		if len(parts) >= 1 {
			jobIDs[parts[0]] = true
		}
	}

	// Generate summary for each job
	for jobID := range jobIDs {
		jobMap[jobID] = r.GetJobSummary(jobID)
	}

	return jobMap
}

// AggregateResultsByType aggregates results by job type
func (r *Runner) AggregateResultsByType() map[JobType]map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	aggregated := make(map[JobType]map[string]interface{})

	for key, result := range r.results {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if jobTypeStr, hasType := resultMap["job_type"]; hasType {
				if typeStr, ok := jobTypeStr.(string); ok {
					jobType := JobType(typeStr)
					if aggregated[jobType] == nil {
						aggregated[jobType] = make(map[string]interface{})
					}
					aggregated[jobType][key] = result
				}
			}
		}
	}

	return aggregated
}

// GetSuccessfulResults returns only successful results
func (r *Runner) GetSuccessfulResults() map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	successful := make(map[string]interface{})

	for key, result := range r.results {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if _, hasError := resultMap["error"]; !hasError {
				successful[key] = result
			}
		} else {
			// If it's not a map with error field, assume it's successful
			successful[key] = result
		}
	}

	return successful
}

// GetFailedResults returns only failed results with errors
func (r *Runner) GetFailedResults() map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	failed := make(map[string]interface{})

	for key, result := range r.results {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if _, hasError := resultMap["error"]; hasError {
				failed[key] = result
			}
		}
	}

	return failed
}

// ==================== Queue Management Methods ====================

// EnqueueGovIDs adds govIDs to the processing queue
func (r *Runner) EnqueueGovIDs(govIDs []string) error {
	if r.queueState == nil {
		return fmt.Errorf("queue state not initialized")
	}

	// Add to queue state for persistence/tracking
	r.queueState.EnqueueGovIDs(govIDs)
	log.Printf("📥 Enqueued %d govIDs to queue state", len(govIDs))

	// Auto-save if enabled
	if r.autoSaveQueue {
		if err := r.SaveQueueState(); err != nil {
			log.Printf("⚠️  Failed to auto-save queue state: %v", err)
		}
	}

	// Immediately submit jobs to the job queue if auto-processing is enabled
	if r.autoProcessQueue {
		log.Printf("🚀 Immediately submitting %d govIDs as pipeline jobs", len(govIDs))

		// Dequeue the govIDs we just enqueued to remove them from pending
		govIDsToSubmit := r.queueState.DequeueNextBatch(len(govIDs))

		// Submit govIDs in batches of 6
		batchSize := 6
		for i := 0; i < len(govIDsToSubmit); i += batchSize {
			end := i + batchSize
			if end > len(govIDsToSubmit) {
				end = len(govIDsToSubmit)
			}
			batch := govIDsToSubmit[i:end]

			// Create pipeline job for batch
			jobID := r.SubmitPipelineJobForGovIDs(batch)

			// Mark all govIDs in batch as processing
			// Note: We can't determine which worker will pick it up since we're using a shared queue
			for _, govID := range batch {
				r.queueState.MarkProcessing(govID, "pending-worker-assignment")
			}

			log.Printf("📋 Created pipeline job %s for %d govIDs: %v", jobID, len(batch), batch)
		}

		// Save queue state after submitting jobs
		if r.autoSaveQueue {
			if err := r.SaveQueueState(); err != nil {
				log.Printf("⚠️  Failed to auto-save queue state after job submission: %v", err)
			}
		}
	}

	return nil
}

// DequeueNextBatch retrieves the next batch of govIDs to process
func (r *Runner) DequeueNextBatch(batchSize int) []string {
	if r.queueState == nil {
		return []string{}
	}

	batch := r.queueState.DequeueNextBatch(batchSize)
	log.Printf("📤 Dequeued %d govIDs from queue", len(batch))

	// Auto-save if enabled
	if r.autoSaveQueue && len(batch) > 0 {
		if err := r.SaveQueueState(); err != nil {
			log.Printf("⚠️  Failed to auto-save queue state: %v", err)
		}
	}

	return batch
}

// MarkGovIDCompleted marks a govID as successfully completed
func (r *Runner) MarkGovIDCompleted(govID string) {
	if r.queueState == nil {
		return
	}

	r.queueState.MarkCompleted(govID)

	log.Printf("✅ Marked govID %s as completed", govID)

	// Auto-save if enabled
	if r.autoSaveQueue {
		if err := r.SaveQueueState(); err != nil {
			log.Printf("⚠️  Failed to auto-save queue state: %v", err)
		}
	}
}

// MarkGovIDFailed marks a govID as failed with an error message
func (r *Runner) MarkGovIDFailed(govID string, errorMsg string) {
	if r.queueState == nil {
		return
	}

	r.queueState.MarkFailed(govID, errorMsg)

	log.Printf("❌ Marked govID %s as failed: %s", govID, errorMsg)

	// Auto-save if enabled
	if r.autoSaveQueue {
		if err := r.SaveQueueState(); err != nil {
			log.Printf("⚠️  Failed to auto-save queue state: %v", err)
		}
	}
}

// RetryFailedGovIDs moves failed govIDs back to pending queue
func (r *Runner) RetryFailedGovIDs(govIDs []string) error {
	if r.queueState == nil {
		return fmt.Errorf("queue state not initialized")
	}

	r.queueState.RetryFailed(govIDs)
	log.Printf("🔄 Retrying %d failed govIDs", len(govIDs))

	// Auto-save if enabled
	if r.autoSaveQueue {
		if err := r.SaveQueueState(); err != nil {
			return fmt.Errorf("failed to save queue state after retry: %w", err)
		}
	}

	return nil
}

// SaveQueueState saves the current queue state to disk
func (r *Runner) SaveQueueState() error {
	if r.queueState == nil {
		return fmt.Errorf("queue state not initialized")
	}

	if err := r.queueState.SaveToFile(r.queueStatePath); err != nil {
		return fmt.Errorf("failed to save queue state: %w", err)
	}

	log.Printf("💾 Queue state saved to %s", r.queueStatePath)
	return nil
}

// LoadQueueState loads queue state from disk
func (r *Runner) LoadQueueState() error {
	loadedState, err := LoadQueueStateFromFile(r.queueStatePath)
	if err != nil {
		return fmt.Errorf("failed to load queue state: %w", err)
	}

	r.queueState = loadedState
	log.Printf("📂 Queue state loaded from %s", r.queueStatePath)
	return nil
}

// GetQueueStats returns statistics about the queue
func (r *Runner) GetQueueStats() QueueStats {
	if r.queueState == nil {
		return QueueStats{}
	}

	return r.queueState.GetStats()
}

// GetLoadBalanceStats returns statistics about load balancing
func (r *Runner) GetLoadBalanceStats() LoadBalanceStats {
	if r.loadBalancer == nil {
		return LoadBalanceStats{}
	}

	return r.loadBalancer.GetLoadBalanceStats()
}

// PrintQueueStats prints queue statistics
func (r *Runner) PrintQueueStats() {
	stats := r.GetQueueStats()
	fmt.Printf("\n")
	fmt.Printf("Queue Statistics:\n")
	fmt.Printf("  Name: %s\n", stats.Name)
	fmt.Printf("  Total GovIDs: %d\n", stats.TotalGovIDs)
	fmt.Printf("  Pending: %d\n", stats.Pending)
	fmt.Printf("  Processing: %d\n", stats.Processing)
	fmt.Printf("  Completed: %d\n", stats.Completed)
	fmt.Printf("  Failed: %d\n", stats.Failed)
	fmt.Printf("  Completion Rate: %.2f%%\n", stats.CompletionRate)
	fmt.Printf("  Created: %s\n", stats.CreatedAt.Format(time.RFC3339))
	fmt.Printf("  Updated: %s\n", stats.UpdatedAt.Format(time.RFC3339))
	fmt.Printf("\n")
}

// PrintLoadBalanceStats prints load balancing statistics
func (r *Runner) PrintLoadBalanceStats() {
	if r.loadBalancer == nil {
		fmt.Println("Load balancer not initialized")
		return
	}
	r.loadBalancer.PrintStats()
}

// AssignGovIDToWorker is deprecated - workers now pull jobs from a shared queue
// This method is kept for backwards compatibility but no longer pre-assigns workers
// Returns a placeholder worker ID
func (r *Runner) AssignGovIDToWorker(govID string) string {
	// With shared queue architecture, we can't pre-assign workers
	// Jobs are picked up by the next idle worker
	log.Printf("⚠️  AssignGovIDToWorker is deprecated - using shared queue architecture")
	return "shared-queue"
}

// startQueueWorker starts a background goroutine that automatically processes queue items
// On startup, it recovers any jobs that were in "processing" state and resubmits pending jobs
func (r *Runner) startQueueWorker() {
	if !r.autoProcessQueue {
		return
	}

	log.Printf("✅ Automatic queue processing enabled - jobs will be submitted immediately when added to queue")
	log.Printf("💡 No polling needed - using event-driven job submission")

	// Recover queue state on startup
	if r.queueState != nil {
		// Requeue all jobs that were marked as "processing" when the runner was last stopped
		// These jobs are no longer actually being processed, so move them back to pending
		requeuedCount := r.queueState.RequeueProcessing("")
		if requeuedCount > 0 {
			log.Printf("♻️  Requeued %d stuck jobs from processing back to pending", requeuedCount)
		}

		// Get all pending govIDs and submit them as jobs
		pendingCount := len(r.queueState.PendingGovIDs)
		if pendingCount > 0 {
			log.Printf("🔄 Submitting %d pending govIDs from queue state", pendingCount)

			// Get a copy of all pending govIDs
			r.queueState.mu.RLock()
			pendingGovIDs := make([]string, len(r.queueState.PendingGovIDs))
			copy(pendingGovIDs, r.queueState.PendingGovIDs)
			r.queueState.mu.RUnlock()

			// Dequeue and submit each govID
			for len(pendingGovIDs) > 0 {
				batchSize := r.queueBatchSize
				if batchSize > len(pendingGovIDs) {
					batchSize = len(pendingGovIDs)
				}

				batch := r.queueState.DequeueNextBatch(batchSize)
				if len(batch) == 0 {
					break
				}

				// Submit dequeued batch in sub-batches of 5
				subBatchSize := 5
				for i := 0; i < len(batch); i += subBatchSize {
					end := i + subBatchSize
					if end > len(batch) {
						end = len(batch)
					}
					subBatch := batch[i:end]

					// Create pipeline job for sub-batch
					jobID := r.SubmitPipelineJobForGovIDs(subBatch)

					// Mark all govIDs in sub-batch as processing
					for _, govID := range subBatch {
						r.queueState.MarkProcessing(govID, "pending-worker-assignment")
					}

					log.Printf("📋 Recovered and submitted pipeline job %s for %d govIDs: %v", jobID, len(subBatch), subBatch)
				}

				// Update remaining count
				r.queueState.mu.RLock()
				pendingGovIDs = r.queueState.PendingGovIDs
				r.queueState.mu.RUnlock()
			}

			log.Printf("✅ All pending govIDs have been submitted to workers")
		}

		// Save queue state after recovery
		if r.autoSaveQueue {
			if err := r.SaveQueueState(); err != nil {
				log.Printf("⚠️  Failed to save queue state after recovery: %v", err)
			}
		}
	}
}

// startCaselistPoller starts a background goroutine that polls for caselists
func (r *Runner) startCaselistPoller(interval time.Duration, firstRunOnly bool) {
	ctx, cancel := context.WithCancel(r.ctx)
	r.caselistPollerCancel = cancel

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Run once immediately
		r.runCaselistPoll()

		if firstRunOnly {
			return
		}

		// Continue polling
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.runCaselistPoll()
			}
		}
	}()
}

// runCaselistPoll fetches the caselist and enqueues all govIDs
func (r *Runner) runCaselistPoll() {
	log.Printf("🔍 Starting caselist poll...")

	// Submit caselist job
	jobID := r.SubmitCaseListJob()

	// Wait for result (timeout after 10 minutes)
	resultKey := fmt.Sprintf("%s_caselist", jobID)
	timeout := time.After(10 * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			log.Printf("❌ Caselist poll timed out")
			return
		case <-ticker.C:
			result, exists := r.GetResult(resultKey)
			if !exists {
				continue
			}

			// Extract govIDs from array
			caseArray, ok := result.([]interface{})
			if !ok {
				log.Printf("❌ Unexpected result format: %T", result)
				return
			}

			govIDs := make([]string, 0, len(caseArray))
			for _, item := range caseArray {
				caseObj, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				// Extract the case_govid field specifically
				if govID, ok := caseObj["case_govid"].(string); ok && govID != "" {
					govIDs = append(govIDs, govID)
				}
			}

			log.Printf("📋 Found %d govIDs from caselist", len(govIDs))
			if len(govIDs) > 0 {
				if err := r.EnqueueGovIDs(govIDs); err != nil {
					log.Printf("❌ Failed to enqueue govIDs: %v", err)
				} else {
					log.Printf("✅ Enqueued %d govIDs", len(govIDs))
				}
			}
			return
		}
	}
}

// Stop gracefully stops all workers and cleans up resources
func (r *Runner) Stop() {
	log.Println("Stopping runner...")

	// Save queue state before shutdown
	if r.queueState != nil && r.queueStatePath != "" {
		log.Println("💾 Saving queue state before shutdown...")
		if err := r.SaveQueueState(); err != nil {
			log.Printf("⚠️  Failed to save queue state: %v", err)
		} else {
			log.Println("✅ Queue state saved successfully")
		}
	}

	// Stop caselist poller
	if r.caselistPollerCancel != nil {
		r.caselistPollerCancel()
	}

	r.cancel()

	// Stop all workers
	for _, worker := range r.workers {
		worker.cancel()
	}

	// Wait for workers to finish current jobs
	time.Sleep(1 * time.Second)

	r.wg.Wait()

	// Stop API server
	if err := r.StopAPIServer(); err != nil {
		log.Printf("Warning: failed to stop API server: %v", err)
	}

	// Stop dokito-backend container
	if err := r.stopDokitoBackend(); err != nil {
		log.Printf("Warning: failed to stop dokito-backend: %v", err)
	}

	// Close docker client
	if r.dokitoClient != nil {
		r.dokitoClient.Close()
	}

	log.Println("Runner stopped")
}

// StartAPIServer starts the HTTP API server
func (r *Runner) StartAPIServer(port int) error {
	if !r.apiEnabled {
		return nil
	}

	r.apiPort = port

	// Create API server
	apiServer := NewAPIServer(r)
	mux := apiServer.SetupRoutes()

	// Apply middleware
	handler := apiServer.loggingMiddleware(mux)
	handler = apiServer.corsMiddleware(handler)

	// Create HTTP server
	r.httpServer = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("🌐 API Server starting on port %d", port)
		log.Printf("📡 API Endpoints:")
		log.Printf("   GET  http://localhost:%d/api/health", port)
		log.Printf("   GET  http://localhost:%d/api/queue/status", port)
		log.Printf("   POST http://localhost:%d/api/queue/add", port)
		log.Printf("   POST http://localhost:%d/api/queue/process", port)
		log.Printf("   POST http://localhost:%d/api/queue/retry", port)
		log.Printf("   GET  http://localhost:%d/api/stats", port)
		log.Printf("   GET  http://localhost:%d/api/jobs", port)

		if err := r.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("❌ API Server error: %v", err)
		}
	}()

	// Give server a moment to start
	time.Sleep(100 * time.Millisecond)
	log.Printf("✅ API Server ready")

	return nil
}

// StopAPIServer gracefully shuts down the HTTP API server
func (r *Runner) StopAPIServer() error {
	if r.httpServer == nil {
		return nil
	}

	log.Println("Stopping API server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Attempt graceful shutdown
	if err := r.httpServer.Shutdown(ctx); err != nil {
		log.Printf("API server shutdown error: %v", err)
		return err
	}

	log.Println("✅ API server stopped")
	return nil
}

// GetStats returns overall runner statistics
func (r *Runner) GetStats() map[string]interface{} {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	totalResults := len(r.results)
	successful := 0
	failed := 0

	jobTypes := make(map[JobType]int)

	for _, result := range r.results {
		if resultMap, ok := result.(map[string]interface{}); ok {
			if _, hasError := resultMap["error"]; hasError {
				failed++
			} else {
				successful++
			}

			if jobTypeStr, hasType := resultMap["job_type"]; hasType {
				if typeStr, ok := jobTypeStr.(string); ok {
					jobType := JobType(typeStr)
					jobTypes[jobType]++
				}
			}
		} else {
			successful++
		}
	}

	return map[string]interface{}{
		"total_results":   totalResults,
		"successful":      successful,
		"failed":          failed,
		"success_rate":    float64(successful) / float64(totalResults) * 100,
		"active_workers":  len(r.workers),
		"job_types":       jobTypes,
		"scraper_image":   r.scraperImage,
		"dokito_base_url": r.dokitoBaseURL,
	}
}

// PipelineProgress tracks the progress of pipeline jobs
type PipelineProgress struct {
	JobID           string                        `json:"job_id"`
	TotalStages     int                           `json:"total_stages"`
	CompletedStages int                           `json:"completed_stages"`
	CurrentStage    PipelineStage                 `json:"current_stage"`
	StageResults    map[PipelineStage]interface{} `json:"stage_results"`
	Errors          []string                      `json:"errors"`
	StartTime       time.Time                     `json:"start_time"`
	LastUpdate      time.Time                     `json:"last_update"`
	Status          string                        `json:"status"` // "running", "completed", "failed"
}

// TrackPipelineProgress tracks progress for pipeline jobs
func (r *Runner) TrackPipelineProgress(jobID string, stage PipelineStage, result interface{}, err error) {
	r.resultsMux.Lock()
	defer r.resultsMux.Unlock()

	progressKey := fmt.Sprintf("pipeline_progress_%s", jobID)
	var progress *PipelineProgress

	if existing, exists := r.results[progressKey]; exists {
		if p, ok := existing.(*PipelineProgress); ok {
			progress = p
		}
	}

	if progress == nil {
		progress = &PipelineProgress{
			JobID:        jobID,
			StageResults: make(map[PipelineStage]interface{}),
			Errors:       []string{},
			StartTime:    time.Now(),
			Status:       "running",
		}
	}

	progress.CurrentStage = stage
	progress.LastUpdate = time.Now()

	if err != nil {
		progress.Errors = append(progress.Errors, fmt.Sprintf("%s: %v", stage, err))
		progress.Status = "failed"
	} else {
		progress.StageResults[stage] = result
		progress.CompletedStages++

		// Check if all stages are completed
		if progress.CompletedStages >= progress.TotalStages {
			progress.Status = "completed"
		}
	}

	r.results[progressKey] = progress
	log.Printf("Pipeline progress updated for job %s: stage %s, status %s (%d/%d stages)",
		jobID, stage, progress.Status, progress.CompletedStages, progress.TotalStages)
}

// GetPipelineProgress returns progress information for a pipeline job
func (r *Runner) GetPipelineProgress(jobID string) *PipelineProgress {
	r.resultsMux.RLock()
	defer r.resultsMux.RUnlock()

	progressKey := fmt.Sprintf("pipeline_progress_%s", jobID)
	if existing, exists := r.results[progressKey]; exists {
		if progress, ok := existing.(*PipelineProgress); ok {
			return progress
		}
	}
	return nil
}

// PrintResults prints all collected results as JSON
func (r *Runner) PrintResults() {
	results := r.GetResults()
	jsonData, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Printf("Error marshaling results: %v", err)
		return
	}
	fmt.Println(string(jsonData))
}

// PrintJobSummary prints a summary of job execution
func (r *Runner) PrintJobSummary(jobID string) {
	summary := r.GetJobSummary(jobID)
	jsonData, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		log.Printf("Error marshaling job summary: %v", err)
		return
	}
	fmt.Printf("Job Summary for %s:\n%s\n", jobID, string(jsonData))
}

// PrintStats prints overall runner statistics
func (r *Runner) PrintStats() {
	stats := r.GetStats()
	jsonData, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		log.Printf("Error marshaling stats: %v", err)
		return
	}
	fmt.Printf("Runner Statistics:\n%s\n", string(jsonData))
}

func main() {
	// Parse command-line flags
	pipelineMode := flag.Bool("pipeline", false, "Execute full pipeline (scrape -> upload -> process -> ingest)")
	govIDsFlag := flag.String("govids", "", "Comma-separated list of Gov IDs to process")
	helpFlag := flag.Bool("help", false, "Show help information")

	// Queue management flags
	queueAddFlag := flag.String("queue-add", "", "Add comma-separated Gov IDs to the queue")
	queueStatusFlag := flag.Bool("queue-status", false, "Show current queue status")
	queueLoadFlag := flag.String("queue-load", "", "Load queue state from file")
	queueSaveFlag := flag.String("queue-save", "", "Save queue state to file")
	queueRetryFlag := flag.Bool("queue-retry", false, "Retry all failed Gov IDs in queue")
	queueProcessFlag := flag.Bool("queue-process", false, "Start processing Gov IDs from the queue")
	queueBatchSize := flag.Int("queue-batch-size", 6, "Number of Gov IDs to process in each batch")

	// API server flags
	apiPort := flag.Int("api-port", 8080, "HTTP API server port")
	noAPI := flag.Bool("no-api", false, "Disable HTTP API server")

	// Auto-processing flags
	noAutoProcess := flag.Bool("no-auto-process", false, "Disable automatic queue processing in daemon mode")
	autoBatchSize := flag.Int("auto-batch-size", 5, "Batch size for automatic queue processing")
	autoPollInterval := flag.Duration("auto-poll-interval", 5*time.Second, "Queue polling interval for auto-processing")

	// Caselist polling flags
	enableCaselistPolling := flag.Bool("enable-caselist-polling", false, "Enable automatic caselist polling")
	caselistFirstRunOnly := flag.Bool("caselist-first-run-only", true, "Run caselist poll only once")
	caselistPollInterval := flag.Duration("caselist-poll-interval", 1*time.Hour, "Caselist polling interval")

	flag.Parse()

	if *helpFlag {
		fmt.Println("JobRunner - End-to-End Document Processing Pipeline")
		fmt.Println("")
		fmt.Println("Usage:")
		fmt.Println("  ./runner                                    # Run in daemon mode with API")
		fmt.Println("  ./runner -pipeline -govids 25-01799        # Execute pipeline for single Gov ID")
		fmt.Println("  ./runner -pipeline -govids 25-01799,25-01548,25-E-0365  # Execute pipeline for multiple Gov IDs")
		fmt.Println("")
		fmt.Println("Queue Management:")
		fmt.Println("  ./runner -queue-add 25-01799,25-01548      # Add Gov IDs to queue")
		fmt.Println("  ./runner -queue-status                      # Show queue status")
		fmt.Println("  ./runner -queue-process                     # Process Gov IDs from queue")
		fmt.Println("  ./runner -queue-process -queue-batch-size 6  # Process queue in batches of 6")
		fmt.Println("  ./runner -queue-retry                       # Retry all failed Gov IDs")
		fmt.Println("  ./runner -queue-save queue.json             # Save queue state to file")
		fmt.Println("  ./runner -queue-load queue.json             # Load queue state from file")
		fmt.Println("")
		fmt.Println("HTTP API Server:")
		fmt.Println("  ./runner -api-port 8080                     # Start with API on port 8080 (default)")
		fmt.Println("  ./runner -no-api                            # Run without API server")
		fmt.Println("  curl http://localhost:8080/api/health       # Check API health")
		fmt.Println("  curl http://localhost:8080/api/queue/status # Get queue status")
		fmt.Println("  curl -X POST http://localhost:8080/api/queue/add \\")
		fmt.Println("    -d '{\"gov_ids\": [\"25-01799\"]}'          # Add govIDs via API")
		fmt.Println("")
		fmt.Println("Pipeline stages: scrape -> upload -> process -> ingest")
		fmt.Println("Uses REAL dokito backend (no mocks)")
		return
	}

	// Read configuration from environment variables or use defaults
	workerCount := 5
	scraperImage := os.Getenv("SCRAPER_IMAGE")
	if scraperImage == "" {
		scraperImage = "jobrunner-worker:latest"
	}

	workDir := os.Getenv("WORK_DIR")
	if workDir == "" {
		workDir = "./output"
	}

	// Convert to absolute path for Docker mount compatibility
	absWorkDir, err := filepath.Abs(workDir)
	if err != nil {
		log.Fatalf("Failed to convert work directory to absolute path: %v", err)
	}
	workDir = absWorkDir

	// ALWAYS use REAL dokito backend - NO MOCKS
	dokitoBaseURL := os.Getenv("DOKITO_BACKEND_URL")
	if dokitoBaseURL == "" {
		dokitoBaseURL = "http://localhost:8123"
	}

	// Create runner
	runner, err := NewRunner(workerCount, scraperImage, workDir, dokitoBaseURL)
	if err != nil {
		log.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Stop()

	// Disable API if requested
	if *noAPI {
		runner.apiEnabled = false
	}

	// Apply auto-processing configuration
	if *noAutoProcess {
		runner.autoProcessQueue = false
	}
	runner.queueBatchSize = *autoBatchSize
	runner.queuePollInterval = *autoPollInterval

	log.Printf("🚀 JobRunner started with %d workers", workerCount)
	log.Printf("📁 Work directory: %s", workDir)
	log.Printf("🐳 Scraper image: %s", scraperImage)
	log.Printf("🔗 Dokito backend: %s (REAL SERVER)", dokitoBaseURL)

	// Pipeline execution mode
	if *pipelineMode {
		if *govIDsFlag == "" {
			log.Fatal("❌ Pipeline mode requires --govids parameter")
		}

		govIDs := strings.Split(*govIDsFlag, ",")
		for i, id := range govIDs {
			govIDs[i] = strings.TrimSpace(id)
		}

		log.Printf("🔄 Executing REAL pipeline for Gov IDs: %v", govIDs)
		log.Printf("📊 Pipeline stages: scrape -> upload -> process -> ingest")
		log.Printf("⚠️  Using REAL dokito backend: %s", dokitoBaseURL)

		// Execute REAL pipeline
		jobID := runner.SubmitPipelineJobForGovIDs(govIDs)
		log.Printf("✅ Pipeline job submitted: %s", jobID)

		// Wait for completion and monitor progress
		timeout := time.After(30 * time.Minute) // 30 minute timeout for full pipeline
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-timeout:
				log.Printf("❌ Pipeline timed out after 30 minutes")
				summary := runner.GetJobSummary(jobID)
				log.Printf("📊 Final Summary: %+v", summary)
				os.Exit(1)
			case <-ticker.C:
				summary := runner.GetJobSummary(jobID)
				log.Printf("📈 Pipeline Progress: Job %s - Success: %d, Failed: %d, Total: %d",
					jobID, summary.Successful, summary.Failed, summary.TotalGovIDs)

				if summary.Successful+summary.Failed >= len(govIDs)*4 { // 4 stages per govID
					log.Printf("✅ Pipeline completed!")
					log.Printf("📊 Final Results: %+v", summary)
					if summary.Failed > 0 {
						log.Printf("⚠️  Some stages failed - check logs")
						os.Exit(1)
					}
					log.Printf("🎉 All pipeline stages completed successfully!")
					return
				}
			}
		}
	}

	// Queue management operations
	if *queueLoadFlag != "" {
		log.Printf("📂 Loading queue state from %s", *queueLoadFlag)
		loadedState, err := LoadQueueStateFromFile(*queueLoadFlag)
		if err != nil {
			log.Fatalf("❌ Failed to load queue state: %v", err)
		}
		runner.queueState = loadedState
		log.Printf("✅ Queue state loaded successfully")
		runner.PrintQueueStats()
	}

	if *queueAddFlag != "" {
		govIDs := strings.Split(*queueAddFlag, ",")
		for i, id := range govIDs {
			govIDs[i] = strings.TrimSpace(id)
		}

		log.Printf("📥 Adding %d Gov IDs to queue", len(govIDs))
		if err := runner.EnqueueGovIDs(govIDs); err != nil {
			log.Fatalf("❌ Failed to enqueue Gov IDs: %v", err)
		}
		runner.PrintQueueStats()

		// If no process flag, exit after adding
		if !*queueProcessFlag {
			return
		}
	}

	if *queueRetryFlag {
		log.Printf("🔄 Retrying all failed Gov IDs")
		if err := runner.RetryFailedGovIDs([]string{}); err != nil {
			log.Fatalf("❌ Failed to retry Gov IDs: %v", err)
		}
		runner.PrintQueueStats()
	}

	if *queueStatusFlag {
		log.Printf("📊 Queue Status:")
		runner.PrintQueueStats()
		runner.PrintLoadBalanceStats()
		return
	}

	if *queueSaveFlag != "" {
		log.Printf("💾 Saving queue state to %s", *queueSaveFlag)
		if err := runner.queueState.SaveToFile(*queueSaveFlag); err != nil {
			log.Fatalf("❌ Failed to save queue state: %v", err)
		}
		log.Printf("✅ Queue state saved successfully")
		return
	}

	if *queueProcessFlag {
		log.Printf("🔄 Processing Gov IDs from queue")
		log.Printf("📦 Batch size: %d", *queueBatchSize)

		// Process queue in batches
		for !runner.queueState.IsEmpty() {
			batch := runner.DequeueNextBatch(*queueBatchSize)
			if len(batch) == 0 {
				break
			}

			log.Printf("📊 Processing batch of %d Gov IDs: %v", len(batch), batch)

			// Submit batch in sub-batches of 5 (will be picked up by next idle worker)
			subBatchSize := 5
			for i := 0; i < len(batch); i += subBatchSize {
				end := i + subBatchSize
				if end > len(batch) {
					end = len(batch)
				}
				subBatch := batch[i:end]

				// Create pipeline job for sub-batch
				jobID := runner.SubmitPipelineJobForGovIDs(subBatch)

				// Mark all govIDs in sub-batch as processing
				for _, govID := range subBatch {
					runner.queueState.MarkProcessing(govID, "shared-queue")
				}

				log.Printf("📋 Created pipeline job %s for %d govIDs: %v", jobID, len(subBatch), subBatch)
			}

			// No sleep - jobs are submitted to the queue and workers will process them continuously
			// Show progress
			runner.PrintQueueStats()
		}

		log.Printf("✅ Queue processing complete!")
		runner.PrintQueueStats()
		return
	}

	// Start API server in daemon mode
	if runner.apiEnabled {
		if err := runner.StartAPIServer(*apiPort); err != nil {
			log.Fatalf("Failed to start API server: %v", err)
		}
	}

	// Start automatic queue processor in daemon mode
	runner.startQueueWorker()

	// Start caselist poller if enabled
	if *enableCaselistPolling {
		runner.startCaselistPoller(*caselistPollInterval, *caselistFirstRunOnly)
		if *caselistFirstRunOnly {
			log.Printf("🔍 Caselist polling: one-time run")
		} else {
			log.Printf("🔍 Caselist polling enabled (interval: %v)", *caselistPollInterval)
		}
	}

	// Daemon mode - keep runner alive
	log.Println("✅ Runner ready to accept jobs (daemon mode)")
	if runner.apiEnabled {
		log.Printf("💡 Use HTTP API at http://localhost:%d/api/", *apiPort)
		log.Printf("💡 Try: curl http://localhost:%d/api/health", *apiPort)
	}
	select {}
}
