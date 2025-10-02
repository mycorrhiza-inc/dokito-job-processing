package main

import (
	"time"
)

// JobStatus represents the current state of a job
type JobStatus string

const (
	JobStatusPending    JobStatus = "pending"
	JobStatusRunning    JobStatus = "running"
	JobStatusCompleted  JobStatus = "completed"
	JobStatusFailed     JobStatus = "failed"
	JobStatusCancelled  JobStatus = "cancelled"
)

// ScrapingMode represents different scraping modes from the Playwright scraper and Dokito backend API
type ScrapingMode string

const (
	// Playwright scraping modes
	ScrapingModeFull               ScrapingMode = "full"
	ScrapingModeMetadata          ScrapingMode = "meta"
	ScrapingModeDocuments         ScrapingMode = "docs"
	ScrapingModeParties           ScrapingMode = "parties"
	ScrapingModeDates             ScrapingMode = "dates"
	ScrapingModeFullExtraction    ScrapingMode = "full-extraction"
	ScrapingModeFilingsBetweenDates ScrapingMode = "filings-between-dates"
	ScrapingModeCaseList          ScrapingMode = "case-list"
	
	// Pipeline modes for end-to-end processing
	ScrapingModePipelineIngest     ScrapingMode = "pipeline-ingest"     // Full scrape → upload → process → ingest
	ScrapingModePipelineUpload     ScrapingMode = "pipeline-upload"     // Scrape → upload raw data only
	ScrapingModePipelineProcess    ScrapingMode = "pipeline-process"    // Process existing raw data
	ScrapingModePipelineStage      ScrapingMode = "pipeline-stage"      // Single pipeline stage execution
	
	// Dokito backend API modes
	ScrapingModeDokitoCaseFetch     ScrapingMode = "dokito-case-fetch"
	ScrapingModeDokitoCaseList      ScrapingMode = "dokito-caselist"
	ScrapingModeDokitoAttachmentObj ScrapingMode = "dokito-attachment-obj"
	ScrapingModeDokitoAttachmentRaw ScrapingMode = "dokito-attachment-raw"
	ScrapingModeDokitoCaseSubmit    ScrapingMode = "dokito-case-submit"
	ScrapingModeDokitoReprocess     ScrapingMode = "dokito-reprocess"
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// LogEntry represents a single log entry for a job with pipeline support
type LogEntry struct {
	ID             string                 `json:"id"`
	Timestamp      time.Time              `json:"timestamp"`
	Level          LogLevel               `json:"level"`
	Message        string                 `json:"message"`
	WorkerID       string                 `json:"worker_id,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	
	// Pipeline-specific fields
	JobID          string `json:"job_id,omitempty"`
	ParentJobID    string `json:"parent_job_id,omitempty"`
	PipelineStage  string `json:"pipeline_stage,omitempty"`
	GovID          string `json:"gov_id,omitempty"`
	StageProgress  string `json:"stage_progress,omitempty"` // "started", "progress", "completed", "failed"
	DebugContext   map[string]interface{} `json:"debug_context,omitempty"`
}

// PipelineStageLog tracks detailed information about pipeline stage execution
type PipelineStageLog struct {
	JobID         string                 `json:"job_id"`
	ParentJobID   string                 `json:"parent_job_id,omitempty"`
	Stage         string                 `json:"stage"`
	GovID         string                 `json:"gov_id"`
	Status        string                 `json:"status"` // "started", "running", "completed", "failed", "retrying"
	StartTime     time.Time              `json:"start_time"`
	EndTime       *time.Time             `json:"end_time,omitempty"`
	Duration      string                 `json:"duration,omitempty"`
	Error         string                 `json:"error,omitempty"`
	RetryCount    int                    `json:"retry_count"`
	Result        interface{}            `json:"result,omitempty"`
	DebugData     map[string]interface{} `json:"debug_data,omitempty"`
	WorkerID      string                 `json:"worker_id,omitempty"`
}

// SubTaskStatus represents the status of an individual sub-task
type SubTaskStatus struct {
	ID          string                 `json:"id"`
	GovID       string                 `json:"gov_id,omitempty"`
	Stage       string                 `json:"stage"` // "scrape", "upload", "process", "ingest"
	Status      JobStatus              `json:"status"`
	StartedAt   *time.Time             `json:"started_at,omitempty"`
	CompletedAt *time.Time             `json:"completed_at,omitempty"`
	Error       string                 `json:"error,omitempty"`
	RetryCount  int                    `json:"retry_count"`
	Result      interface{}            `json:"result,omitempty"`
	DebugInfo   map[string]interface{} `json:"debug_info,omitempty"`
}

// PipelineProgress tracks overall pipeline progress and stage-specific metrics
type PipelineProgress struct {
	TotalGovIDs    int                        `json:"total_gov_ids"`
	CurrentStage   string                     `json:"current_stage"`
	StageProgress  map[string]StageProgress   `json:"stage_progress"`
	SubTasks       map[string]SubTaskStatus   `json:"sub_tasks"` // keyed by sub-task ID
	FailedGovIDs   []string                   `json:"failed_gov_ids"`
	CompletedGovIDs []string                  `json:"completed_gov_ids"`
	LastUpdated    time.Time                  `json:"last_updated"`
}

// StageProgress tracks progress within a specific pipeline stage
type StageProgress struct {
	Stage       string    `json:"stage"`
	Pending     int       `json:"pending"`
	Running     int       `json:"running"`
	Completed   int       `json:"completed"`
	Failed      int       `json:"failed"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// Job represents a scraping job with enhanced pipeline support
type Job struct {
	ID          string       `json:"id"`
	Mode        ScrapingMode `json:"mode"`
	GovIDs      []string     `json:"gov_ids,omitempty"`
	DateString  string       `json:"date_string,omitempty"`
	BeginDate   string       `json:"begin_date,omitempty"`
	EndDate     string       `json:"end_date,omitempty"`
	Status      JobStatus    `json:"status"`
	Result      interface{}  `json:"result,omitempty"`
	Error       string       `json:"error,omitempty"`
	CreatedAt   time.Time    `json:"created_at"`
	StartedAt   *time.Time   `json:"started_at,omitempty"`
	CompletedAt *time.Time   `json:"completed_at,omitempty"`
	WorkerID    string       `json:"worker_id,omitempty"`
	
	// Pipeline-specific fields
	IsPipeline      bool              `json:"is_pipeline,omitempty"`
	ParentJobID     string            `json:"parent_job_id,omitempty"`
	PipelineStage   string            `json:"pipeline_stage,omitempty"`
	Progress        *PipelineProgress `json:"progress,omitempty"`
	
	// Dokito-specific parameters
	State            string      `json:"state,omitempty"`
	JurisdictionName string      `json:"jurisdiction_name,omitempty"`
	CaseName         string      `json:"case_name,omitempty"`
	Blake2bHash      string      `json:"blake2b_hash,omitempty"`
	CaseData         interface{} `json:"case_data,omitempty"`
	OperationType    string      `json:"operation_type,omitempty"`
	Limit            int         `json:"limit,omitempty"`
	Offset           int         `json:"offset,omitempty"`
	
	// Debugging and tracking
	DebugInfo       map[string]interface{} `json:"debug_info,omitempty"`
	RetryCount      int                    `json:"retry_count"`
	MaxRetries      int                    `json:"max_retries"`
	DependsOn       []string               `json:"depends_on,omitempty"` // job IDs this job depends on
}

// CreateJobRequest represents the request to create a new job
type CreateJobRequest struct {
	Mode       ScrapingMode `json:"mode" binding:"required"`
	GovIDs     []string     `json:"gov_ids,omitempty"`
	DateString string       `json:"date_string,omitempty"`
	BeginDate  string       `json:"begin_date,omitempty"`
	EndDate    string       `json:"end_date,omitempty"`
	
	// Pipeline-specific parameters
	IsPipeline      bool     `json:"is_pipeline,omitempty"`
	ParentJobID     string   `json:"parent_job_id,omitempty"`
	PipelineStage   string   `json:"pipeline_stage,omitempty"`
	MaxRetries      int      `json:"max_retries,omitempty"`
	DependsOn       []string `json:"depends_on,omitempty"`
	
	// Dokito-specific parameters
	State            string      `json:"state,omitempty"`
	JurisdictionName string      `json:"jurisdiction_name,omitempty"`
	CaseName         string      `json:"case_name,omitempty"`
	Blake2bHash      string      `json:"blake2b_hash,omitempty"`
	CaseData         interface{} `json:"case_data,omitempty"`
	OperationType    string      `json:"operation_type,omitempty"`
	Limit            int         `json:"limit,omitempty"`
	Offset           int         `json:"offset,omitempty"`
}

// JobListResponse represents the response for listing jobs
type JobListResponse struct {
	Jobs  []Job `json:"jobs"`
	Total int   `json:"total"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error string `json:"error"`
}

// HealthResponse represents the health check response
type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp time.Time         `json:"timestamp"`
	Services  map[string]string `json:"services"`
}

// LogsResponse represents the response for retrieving job logs
type LogsResponse struct {
	Logs  []LogEntry `json:"logs"`
	Total int        `json:"total"`
}

// AddLogRequest represents the request to add a log entry to a job
type AddLogRequest struct {
	Level    LogLevel               `json:"level" binding:"required"`
	Message  string                 `json:"message" binding:"required"`
	WorkerID string                 `json:"worker_id,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// ===== DOKITO BACKEND DATA STRUCTURES =====

// ProcessingActionRawData represents actions for raw dockets (used in raw-dockets endpoint)
type ProcessingActionRawData string

const (
	ProcessingActionRawDataProcessOnly      ProcessingActionRawData = "process_only"
	ProcessingActionRawDataProcessAndIngest ProcessingActionRawData = "process_and_ingest"
	ProcessingActionRawDataUploadRaw        ProcessingActionRawData = "upload_raw"
)

// ProcessingActionIdOnly represents actions for ID-based processing (used in by-ids, by-jurisdiction, by-daterange)
type ProcessingActionIdOnly string

const (
	ProcessingActionIdOnlyProcessOnly      ProcessingActionIdOnly = "process_only"
	ProcessingActionIdOnlyIngestOnly       ProcessingActionIdOnly = "ingest_only"
	ProcessingActionIdOnlyProcessAndIngest ProcessingActionIdOnly = "process_and_ingest"
)

// RawDocketsRequest represents a request to submit raw dockets
type RawDocketsRequest struct {
	Action  ProcessingActionRawData `json:"action" binding:"required"`
	Dockets []RawGenericDocket      `json:"dockets" binding:"required"`
}

// ByIdsRequest represents a request to process dockets by IDs
type ByIdsRequest struct {
	DocketIds []string `json:"docket_ids" binding:"required"`
}

// ByJurisdictionRequest represents a request to process all dockets in a jurisdiction
type ByJurisdictionRequest struct {
	Action ProcessingActionIdOnly `json:"action" binding:"required"`
}

// ByDateRangeRequest represents a request to process dockets within a date range
type ByDateRangeRequest struct {
	Action    ProcessingActionIdOnly `json:"action" binding:"required"`
	StartDate string                 `json:"start_date" binding:"required"` // Format: YYYY-MM-DD (will be validated)
	EndDate   string                 `json:"end_date" binding:"required"`   // Format: YYYY-MM-DD (will be validated)
}

// ProcessingResponse represents the response from dokito backend processing
type ProcessingResponse struct {
	SuccessfullyProcessedDockets []CaseRawOrProcessed `json:"successfully_processed_dockets"`
	SuccessCount                 int                  `json:"success_count"`
	ErrorCount                   int                  `json:"error_count"`
}

// CaseRawOrProcessed represents either a raw or processed case
// This matches the Rust enum serialization: either {"Processed": {...}} or {"Raw": {...}}
type CaseRawOrProcessed struct {
	Processed *ProcessedGenericDocket `json:"Processed,omitempty"`
	Raw       *RawGenericDocket       `json:"Raw,omitempty"`
}

// ProcessedGenericDocket represents a processed legal case docket
type ProcessedGenericDocket struct {
	CaseGovID       string                   `json:"case_govid"`
	OpenedDate      string                   `json:"opened_date"`
	ObjectUUID      string                   `json:"object_uuid"`
	CaseName        string                   `json:"case_name"`
	CaseURL         string                   `json:"case_url"`
	CaseType        string                   `json:"case_type"`
	CaseSubtype     string                   `json:"case_subtype"`
	Description     string                   `json:"description"`
	Industry        string                   `json:"industry"`
	PetitionerList  []interface{}            `json:"petitioner_list"`
	HearingOfficer  string                   `json:"hearing_officer"`
	ClosedDate      *string                  `json:"closed_date,omitempty"`
	Filings         []interface{}            `json:"filings"`
	CaseParties     []interface{}            `json:"case_parties"`
	ExtraMetadata   map[string]interface{}   `json:"extra_metadata"`
	IndexedAt       time.Time                `json:"indexed_at"`
	ProcessedAt     time.Time                `json:"processed_at"`
}

// RawGenericDocket represents a raw legal case docket (simplified version)
type RawGenericDocket struct {
	CaseGovID       string                   `json:"case_govid" binding:"required"`
	OpenedDate      *string                  `json:"opened_date,omitempty"`
	CaseName        string                   `json:"case_name"`
	CaseURL         string                   `json:"case_url"`
	CaseType        string                   `json:"case_type"`
	CaseSubtype     string                   `json:"case_subtype"`
	Description     string                   `json:"description"`
	Industry        string                   `json:"industry"`
	Petitioner      string                   `json:"petitioner"`
	HearingOfficer  string                   `json:"hearing_officer"`
	ClosedDate      *string                  `json:"closed_date,omitempty"`
	Filings         []RawGenericFiling       `json:"filings"`
	CaseParties     []GenericParty           `json:"case_parties"`
	ExtraMetadata   map[string]interface{}   `json:"extra_metadata"`
	IndexedAt       time.Time                `json:"indexed_at"`
}

// RawGenericFiling represents a raw filing within a case
type RawGenericFiling struct {
	FiledDate                 *string                  `json:"filed_date,omitempty"`
	FillingGovID              string                   `json:"filling_govid"`
	Name                      string                   `json:"name"`
	OrganizationAuthors       []string                 `json:"organization_authors"`
	IndividualAuthors         []string                 `json:"individual_authors"`
	OrganizationAuthorsBlob   string                   `json:"organization_authors_blob"`
	IndividualAuthorsBlob     string                   `json:"individual_authors_blob"`
	FilingType                string                   `json:"filing_type"`
	Description               string                   `json:"description"`
	Attachments               []RawGenericAttachment   `json:"attachments"`
	ExtraMetadata             map[string]interface{}   `json:"extra_metadata"`
}

// RawGenericAttachment represents a raw attachment to a filing
type RawGenericAttachment struct {
	Name               string                 `json:"name"`
	DocumentExtension  string                 `json:"document_extension"`
	AttachmentGovID    string                 `json:"attachment_govid"`
	URL                string                 `json:"url"`
	AttachmentType     string                 `json:"attachment_type"`
	AttachmentSubtype  string                 `json:"attachment_subtype"`
	ExtraMetadata      map[string]interface{} `json:"extra_metadata"`
	Hash               *string                `json:"hash,omitempty"`
}

// GenericParty represents a party involved in a case
type GenericParty struct {
	Name        string                 `json:"name"`
	Type        string                 `json:"type"`
	Role        string                 `json:"role"`
	Contact     map[string]interface{} `json:"contact"`
	Metadata    map[string]interface{} `json:"metadata"`
}

// ===== PIPELINE-SPECIFIC JOB MODELS =====

// PipelineJob represents a complete pipeline execution with multiple stages
type PipelineJob struct {
	*Job // Embed base Job
	
	// Pipeline configuration
	Stages          []string               `json:"stages"`          // ["scrape", "upload", "process", "ingest"]
	StageConfig     map[string]interface{} `json:"stage_config"`    // Stage-specific configuration
	AutoAdvance     bool                   `json:"auto_advance"`    // Automatically advance to next stage
	FailurePolicy   string                 `json:"failure_policy"`  // "abort", "continue", "retry"
	
	// Execution tracking
	CurrentStageIndex int                  `json:"current_stage_index"`
	CompletedStages   []string             `json:"completed_stages"`
	FailedStages      []string             `json:"failed_stages"`
	StageResults      map[string]interface{} `json:"stage_results"`
	
	// Error handling and debugging
	FailureDetails    map[string]FailureDetail `json:"failure_details,omitempty"`
	RetryHistory      []RetryAttempt           `json:"retry_history,omitempty"`
	LastHealthCheck   *time.Time               `json:"last_health_check,omitempty"`
}

// FailureDetail captures comprehensive information about job failures
type FailureDetail struct {
	Stage          string                 `json:"stage"`
	GovID          string                 `json:"gov_id,omitempty"`
	ErrorCode      string                 `json:"error_code"`
	ErrorMessage   string                 `json:"error_message"`
	StackTrace     string                 `json:"stack_trace,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
	WorkerID       string                 `json:"worker_id,omitempty"`
	
	// Environmental context
	SystemInfo     map[string]interface{} `json:"system_info,omitempty"`
	RequestData    interface{}            `json:"request_data,omitempty"`
	ResponseData   interface{}            `json:"response_data,omitempty"`
	
	// Recovery information
	IsRetryable    bool                   `json:"is_retryable"`
	SuggestedFix   string                 `json:"suggested_fix,omitempty"`
	RelatedIssues  []string               `json:"related_issues,omitempty"`
	
	// Debugging context
	DebugSnapshot  map[string]interface{} `json:"debug_snapshot,omitempty"`
}

// RetryAttempt tracks information about retry attempts
type RetryAttempt struct {
	AttemptNumber  int                    `json:"attempt_number"`
	Timestamp      time.Time              `json:"timestamp"`
	Reason         string                 `json:"reason"`
	Stage          string                 `json:"stage,omitempty"`
	GovID          string                 `json:"gov_id,omitempty"`
	Result         string                 `json:"result"` // "success", "failure", "partial"
	Duration       string                 `json:"duration,omitempty"`
	ErrorMessage   string                 `json:"error_message,omitempty"`
	ChangesApplied []string               `json:"changes_applied,omitempty"`
}

// ScraperTaskConfig configures scraping tasks for NY PUC
type ScraperTaskConfig struct {
	Jurisdiction     string            `json:"jurisdiction"`      // "ny_puc"
	State           string            `json:"state"`             // "ny"
	ScrapingMode    string            `json:"scraping_mode"`     // "full", "metadata", "documents", "parties"
	MaxConcurrent   int               `json:"max_concurrent"`    // Maximum concurrent scrapers
	TimeoutSeconds  int               `json:"timeout_seconds"`   // Timeout per gov ID
	RetryPolicy     RetryPolicy       `json:"retry_policy"`      // Retry configuration
	
	// NY PUC specific settings
	BrowserConfig   map[string]interface{} `json:"browser_config,omitempty"`
	PageLoadTimeout int                   `json:"page_load_timeout,omitempty"`
	ElementTimeout  int                   `json:"element_timeout,omitempty"`
}

// UploadTaskConfig configures upload tasks to dokito backend
type UploadTaskConfig struct {
	BackendURL      string      `json:"backend_url"`
	ProcessingAction string     `json:"processing_action"` // "process_only", "process_and_ingest", "upload_raw"
	BatchSize       int         `json:"batch_size"`        // Number of dockets per batch
	TimeoutSeconds  int         `json:"timeout_seconds"`
	RetryPolicy     RetryPolicy `json:"retry_policy"`
	
	// Validation settings
	ValidateData    bool        `json:"validate_data"`
	RequiredFields  []string    `json:"required_fields,omitempty"`
}

// ProcessTaskConfig configures processing tasks in dokito backend
type ProcessTaskConfig struct {
	BackendURL     string      `json:"backend_url"`
	ProcessingType string      `json:"processing_type"` // "govid", "jurisdiction", "daterange"
	BatchSize      int         `json:"batch_size"`
	TimeoutSeconds int         `json:"timeout_seconds"`
	RetryPolicy    RetryPolicy `json:"retry_policy"`
	
	// Processing options
	ProcessOnly    bool `json:"process_only"`
	IngestOnly     bool `json:"ingest_only"`
	FullOperation  bool `json:"full_operation"`
}

// RetryPolicy defines how failures should be retried
type RetryPolicy struct {
	MaxRetries      int           `json:"max_retries"`
	InitialDelay    time.Duration `json:"initial_delay"`
	MaxDelay        time.Duration `json:"max_delay"`
	BackoffFactor   float64       `json:"backoff_factor"`
	RetryableErrors []string      `json:"retryable_errors,omitempty"`
}

// TaskResult represents the result of a single task execution
type TaskResult struct {
	TaskID        string                 `json:"task_id"`
	Stage         string                 `json:"stage"`
	GovID         string                 `json:"gov_id,omitempty"`
	Status        string                 `json:"status"` // "success", "failure", "partial"
	StartTime     time.Time              `json:"start_time"`
	EndTime       time.Time              `json:"end_time"`
	Duration      time.Duration          `json:"duration"`
	
	// Result data
	Data          interface{}            `json:"data,omitempty"`
	RecordsCount  int                    `json:"records_count,omitempty"`
	ProcessedIDs  []string               `json:"processed_ids,omitempty"`
	FailedIDs     []string               `json:"failed_ids,omitempty"`
	
	// Error information
	Error         string                 `json:"error,omitempty"`
	ErrorCode     string                 `json:"error_code,omitempty"`
	WarningCount  int                    `json:"warning_count,omitempty"`
	Warnings      []string               `json:"warnings,omitempty"`
	
	// Performance metrics
	Metrics       map[string]interface{} `json:"metrics,omitempty"`
	ResourceUsage map[string]interface{} `json:"resource_usage,omitempty"`
}

// CreatePipelineJobRequest represents a request to create a pipeline job
type CreatePipelineJobRequest struct {
	// Basic job information
	GovIDs           []string `json:"gov_ids" binding:"required"`
	State            string   `json:"state" binding:"required"`
	JurisdictionName string   `json:"jurisdiction_name" binding:"required"`
	
	// Pipeline configuration
	PipelineType     string                 `json:"pipeline_type" binding:"required"` // "ingest", "upload", "process"
	Stages           []string               `json:"stages,omitempty"`
	AutoAdvance      bool                   `json:"auto_advance"`
	FailurePolicy    string                 `json:"failure_policy,omitempty"`
	
	// Stage configurations
	ScraperConfig    *ScraperTaskConfig     `json:"scraper_config,omitempty"`
	UploadConfig     *UploadTaskConfig      `json:"upload_config,omitempty"`
	ProcessConfig    *ProcessTaskConfig     `json:"process_config,omitempty"`
	
	// Global settings
	MaxRetries       int                    `json:"max_retries,omitempty"`
	TimeoutMinutes   int                    `json:"timeout_minutes,omitempty"`
	Priority         int                    `json:"priority,omitempty"`
}

