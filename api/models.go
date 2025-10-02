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

// LogEntry represents a single log entry for a job
type LogEntry struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	Message   string    `json:"message"`
	WorkerID  string    `json:"worker_id,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// Job represents a scraping job
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

// CreateJobRequest represents the request to create a new job
type CreateJobRequest struct {
	Mode       ScrapingMode `json:"mode" binding:"required"`
	GovIDs     []string     `json:"gov_ids,omitempty"`
	DateString string       `json:"date_string,omitempty"`
	BeginDate  string       `json:"begin_date,omitempty"`
	EndDate    string       `json:"end_date,omitempty"`
	
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

