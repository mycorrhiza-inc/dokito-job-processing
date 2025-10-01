package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

type DatabaseClient struct {
	db *sql.DB
}

// NewDatabaseClient creates a new database client
func NewDatabaseClient() (*DatabaseClient, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL environment variable is required")
	}

	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	client := &DatabaseClient{db: db}
	
	// Initialize database schema
	if err := client.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize database schema: %w", err)
	}

	log.Println("Database client initialized successfully")
	return client, nil
}

// Close closes the database connection
func (d *DatabaseClient) Close() error {
	return d.db.Close()
}

// initSchema creates the necessary tables if they don't exist
// For Supabase, this assumes the schema has been created manually using supabase-migrations.sql
func (d *DatabaseClient) initSchema() error {
	// Check if tables exist by trying a simple query
	// If tables don't exist, provide helpful error message
	
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'jobs' AND table_schema = 'public'").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check if jobs table exists: %w", err)
	}
	
	if count == 0 {
		log.Println("WARNING: 'jobs' table does not exist. Please run the supabase-migrations.sql script in your Supabase dashboard.")
		log.Println("You can find the migration script at: supabase-migrations.sql")
		return fmt.Errorf("database schema not initialized - please run supabase-migrations.sql")
	}
	
	// Note: job_logs table is no longer required - using file-based logging
	
	log.Println("Database schema validation successful - tables exist")
	return nil
}

// StoreJob stores a job in the database
func (d *DatabaseClient) StoreJob(job *Job) error {
	query := `
		INSERT INTO jobs (
			id, mode, gov_ids, date_string, begin_date, end_date, status,
			created_at, worker_id, state, jurisdiction_name, case_name,
			blake2b_hash, case_data, operation_type, limit_param, offset_param
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (id) DO UPDATE SET
			status = EXCLUDED.status,
			result = EXCLUDED.result,
			error_message = EXCLUDED.error_message,
			started_at = EXCLUDED.started_at,
			completed_at = EXCLUDED.completed_at,
			worker_id = EXCLUDED.worker_id
	`

	var govIDs interface{}
	if job.GovIDs != nil {
		govIDs = pq.Array(job.GovIDs)
	}

	var caseData interface{}
	if job.CaseData != nil {
		caseData = job.CaseData
	}

	_, err := d.db.Exec(query,
		job.ID, job.Mode, govIDs, job.DateString, job.BeginDate, job.EndDate,
		job.Status, job.CreatedAt, job.WorkerID, job.State, job.JurisdictionName,
		job.CaseName, job.Blake2bHash, caseData, job.OperationType,
		job.Limit, job.Offset,
	)

	if err != nil {
		return fmt.Errorf("failed to store job: %w", err)
	}

	return nil
}

// GetJob retrieves a job by ID
func (d *DatabaseClient) GetJob(jobID string) (*Job, error) {
	query := `
		SELECT id, mode, gov_ids, date_string, begin_date, end_date, status,
			   result, error_message, created_at, started_at, completed_at, worker_id,
			   state, jurisdiction_name, case_name, blake2b_hash, case_data,
			   operation_type, limit_param, offset_param
		FROM jobs WHERE id = $1
	`

	row := d.db.QueryRow(query, jobID)
	
	job := &Job{}
	var govIDs interface{}
	var caseData interface{}
	var startedAt sql.NullTime
	var completedAt sql.NullTime
	var workerID sql.NullString
	var dateString sql.NullString
	var beginDate sql.NullString
	var endDate sql.NullString
	var result interface{}
	var errorMsg sql.NullString
	var state sql.NullString
	var jurisdictionName sql.NullString
	var caseName sql.NullString
	var blake2bHash sql.NullString
	var operationType sql.NullString
	var limitParam sql.NullInt64
	var offsetParam sql.NullInt64
	
	err := row.Scan(
		&job.ID, &job.Mode, &govIDs, &dateString, &beginDate, &endDate,
		&job.Status, &result, &errorMsg, &job.CreatedAt, &startedAt,
		&completedAt, &workerID, &state, &jurisdictionName,
		&caseName, &blake2bHash, &caseData, &operationType,
		&limitParam, &offsetParam,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("job not found")
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	// Handle nullable fields
	if dateString.Valid {
		job.DateString = dateString.String
	}
	if beginDate.Valid {
		job.BeginDate = beginDate.String
	}
	if endDate.Valid {
		job.EndDate = endDate.String
	}
	if result != nil {
		job.Result = result
	}
	if errorMsg.Valid {
		job.Error = errorMsg.String
	}
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}
	if workerID.Valid {
		job.WorkerID = workerID.String
	}
	if state.Valid {
		job.State = state.String
	}
	if jurisdictionName.Valid {
		job.JurisdictionName = jurisdictionName.String
	}
	if caseName.Valid {
		job.CaseName = caseName.String
	}
	if blake2bHash.Valid {
		job.Blake2bHash = blake2bHash.String
	}
	if operationType.Valid {
		job.OperationType = operationType.String
	}
	if limitParam.Valid {
		job.Limit = int(limitParam.Int64)
	}
	if offsetParam.Valid {
		job.Offset = int(offsetParam.Int64)
	}

	// Handle array conversion for gov_ids
	if govIDs != nil {
		if govIDsSlice, ok := govIDs.([]interface{}); ok {
			job.GovIDs = make([]string, len(govIDsSlice))
			for i, v := range govIDsSlice {
				if str, ok := v.(string); ok {
					job.GovIDs[i] = str
				}
			}
		}
	}

	// Handle JSON conversion for case_data
	if caseData != nil {
		job.CaseData = caseData
	}

	return job, nil
}

// UpdateJobStatus updates a job's status and related fields
func (d *DatabaseClient) UpdateJobStatus(jobID string, status JobStatus, result interface{}, errorMsg string) error {
	// Convert status to string for consistent typing
	statusStr := string(status)
	
	// Determine timestamp updates in Go to avoid PostgreSQL type issues
	var startedAtUpdate interface{}
	var completedAtUpdate interface{}
	
	// Set started_at if transitioning to running status
	if status == JobStatusRunning {
		startedAtUpdate = "CASE WHEN started_at IS NULL THEN NOW() ELSE started_at END"
	} else {
		startedAtUpdate = "started_at"
	}
	
	// Set completed_at if transitioning to final status
	if status == JobStatusCompleted || status == JobStatusFailed || status == JobStatusCancelled {
		completedAtUpdate = "NOW()"
	} else {
		completedAtUpdate = "completed_at"
	}
	
	query := fmt.Sprintf(`
		UPDATE jobs SET 
			status = $2,
			error_message = $3,
			started_at = %s,
			completed_at = %s
		WHERE id = $1
	`, startedAtUpdate, completedAtUpdate)

	// Handle error message parameter with explicit typing for TEXT
	var errorParam interface{}
	if errorMsg == "" {
		errorParam = nil
	} else {
		errorParam = errorMsg
	}

	log.Printf("DATABASE: Executing update for job %s, status=%s, hasError=%t (result stored in Redis)", 
		jobID, statusStr, errorMsg != "")
	dbResult, err := d.db.Exec(query, jobID, statusStr, errorParam)
	if err != nil {
		log.Printf("DATABASE ERROR: Failed to execute update query for job %s: %v", jobID, err)
		return fmt.Errorf("failed to update job status: %w", err)
	}

	// Check how many rows were affected
	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		log.Printf("DATABASE WARNING: Could not get rows affected for job %s: %v", jobID, err)
	} else {
		log.Printf("DATABASE: Update affected %d rows for job %s", rowsAffected, jobID)
		if rowsAffected == 0 {
			log.Printf("DATABASE WARNING: Job %s was not found in database - no rows updated", jobID)
			return fmt.Errorf("job %s not found in database", jobID)
		}
	}

	log.Printf("DATABASE SUCCESS: Job %s status updated to %s", jobID, status)
	return nil
}

// UpdateJobStatusWithArtifacts updates a job's status and related fields including artifact storage
func (d *DatabaseClient) UpdateJobStatusWithArtifacts(jobID string, status JobStatus, result interface{}, errorMsg string, artifactURL string, artifactMetadata interface{}) error {
	// Convert status to string for consistent typing
	statusStr := string(status)
	
	// Determine timestamp updates in Go to avoid PostgreSQL type issues
	var startedAtUpdate interface{}
	var completedAtUpdate interface{}
	
	// Set started_at if transitioning to running status
	if status == JobStatusRunning {
		startedAtUpdate = "CASE WHEN started_at IS NULL THEN NOW() ELSE started_at END"
	} else {
		startedAtUpdate = "started_at"
	}
	
	// Set completed_at if transitioning to final status
	if status == JobStatusCompleted || status == JobStatusFailed || status == JobStatusCancelled {
		completedAtUpdate = "NOW()"
	} else {
		completedAtUpdate = "completed_at"
	}
	
	query := fmt.Sprintf(`
		UPDATE jobs SET 
			status = $2,
			error_message = $3,
			artifact_storage_url = $4,
			artifact_metadata = $5,
			started_at = %s,
			completed_at = %s
		WHERE id = $1
	`, startedAtUpdate, completedAtUpdate)

	// Handle error message parameter with explicit typing for TEXT
	var errorParam interface{}
	if errorMsg == "" {
		errorParam = nil
	} else {
		errorParam = errorMsg
	}

	// Handle artifact URL as NULL if empty
	var artifactURLParam interface{}
	if artifactURL == "" {
		artifactURLParam = nil
	} else {
		artifactURLParam = artifactURL
	}

	// Handle artifact metadata with explicit typing for JSONB
	var artifactMetadataParam interface{}
	if artifactMetadata == nil {
		artifactMetadataParam = nil
	} else {
		// Ensure metadata can be properly marshaled to JSONB
		metadataBytes, err := json.Marshal(artifactMetadata)
		if err != nil {
			log.Printf("Warning: Failed to marshal artifact metadata for job %s: %v", jobID, err)
			artifactMetadataParam = nil
		} else {
			artifactMetadataParam = string(metadataBytes)
		}
	}

	log.Printf("DATABASE: Executing artifact update for job %s, status=%s, hasArtifact=%t (result stored in Redis)", 
		jobID, statusStr, artifactURL != "")
	dbResult, err := d.db.Exec(query, jobID, statusStr, errorParam, artifactURLParam, artifactMetadataParam)
	if err != nil {
		log.Printf("DATABASE ERROR: Failed to execute artifact update query for job %s: %v", jobID, err)
		return fmt.Errorf("failed to update job status with artifacts: %w", err)
	}

	// Check how many rows were affected
	rowsAffected, err := dbResult.RowsAffected()
	if err != nil {
		log.Printf("DATABASE WARNING: Could not get rows affected for job %s: %v", jobID, err)
	} else {
		log.Printf("DATABASE: Artifact update affected %d rows for job %s", rowsAffected, jobID)
		if rowsAffected == 0 {
			log.Printf("DATABASE WARNING: Job %s was not found in database - no rows updated", jobID)
			return fmt.Errorf("job %s not found in database", jobID)
		}
	}

	log.Printf("DATABASE SUCCESS: Job %s status updated to %s with artifacts", jobID, status)
	return nil
}

// GetAllJobs retrieves jobs with pagination
func (d *DatabaseClient) GetAllJobs(limit, offset int) ([]Job, error) {
	query := `
		SELECT id, mode, gov_ids, date_string, begin_date, end_date, status,
			   result, error_message, created_at, started_at, completed_at, worker_id,
			   state, jurisdiction_name, case_name, blake2b_hash, case_data,
			   operation_type, limit_param, offset_param
		FROM jobs 
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := d.db.Query(query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0) // Initialize as empty slice instead of nil
	for rows.Next() {
		job := Job{}
		var govIDs interface{}
		var caseData interface{}
		var startedAt sql.NullTime
		var completedAt sql.NullTime
		var workerID sql.NullString
		var dateString sql.NullString
		var beginDate sql.NullString
		var endDate sql.NullString
		var result interface{}
		var errorMsg sql.NullString
		var state sql.NullString
		var jurisdictionName sql.NullString
		var caseName sql.NullString
		var blake2bHash sql.NullString
		var operationType sql.NullString
		var limitParam sql.NullInt64
		var offsetParam sql.NullInt64
		
		err := rows.Scan(
			&job.ID, &job.Mode, &govIDs, &dateString, &beginDate, &endDate,
			&job.Status, &result, &errorMsg, &job.CreatedAt, &startedAt,
			&completedAt, &workerID, &state, &jurisdictionName,
			&caseName, &blake2bHash, &caseData, &operationType,
			&limitParam, &offsetParam,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan job row: %w", err)
		}

		// Handle nullable fields
		if dateString.Valid {
			job.DateString = dateString.String
		}
		if beginDate.Valid {
			job.BeginDate = beginDate.String
		}
		if endDate.Valid {
			job.EndDate = endDate.String
		}
		if result != nil {
			job.Result = result
		}
		if errorMsg.Valid {
			job.Error = errorMsg.String
		}
		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}
		if workerID.Valid {
			job.WorkerID = workerID.String
		}
		if state.Valid {
			job.State = state.String
		}
		if jurisdictionName.Valid {
			job.JurisdictionName = jurisdictionName.String
		}
		if caseName.Valid {
			job.CaseName = caseName.String
		}
		if blake2bHash.Valid {
			job.Blake2bHash = blake2bHash.String
		}
		if operationType.Valid {
			job.OperationType = operationType.String
		}
		if limitParam.Valid {
			job.Limit = int(limitParam.Int64)
		}
		if offsetParam.Valid {
			job.Offset = int(offsetParam.Int64)
		}

		// Handle array conversion for gov_ids
		if govIDs != nil {
			if govIDsSlice, ok := govIDs.([]interface{}); ok {
				job.GovIDs = make([]string, len(govIDsSlice))
				for i, v := range govIDsSlice {
					if str, ok := v.(string); ok {
						job.GovIDs[i] = str
					}
				}
			}
		}

		// Handle JSON conversion for case_data
		if caseData != nil {
			job.CaseData = caseData
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating job rows: %w", err)
	}

	return jobs, nil
}

// DeleteJob removes a job from the database
func (d *DatabaseClient) DeleteJob(jobID string) error {
	query := `DELETE FROM jobs WHERE id = $1`
	
	result, err := d.db.Exec(query, jobID)
	if err != nil {
		return fmt.Errorf("failed to delete job: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("job not found")
	}

	return nil
}




// HealthCheck checks if the database is responsive
func (d *DatabaseClient) HealthCheck() error {
	return d.db.Ping()
}

// UpdateJobWorkerID updates the worker_id for a specific job
func (d *DatabaseClient) UpdateJobWorkerID(jobID, workerID string) error {
	query := `UPDATE jobs SET worker_id = $2 WHERE id = $1`
	
	_, err := d.db.Exec(query, jobID, workerID)
	if err != nil {
		return fmt.Errorf("failed to update job worker ID: %w", err)
	}
	
	return nil
}


// GetRunningJobs retrieves all jobs that are currently in running status
func (d *DatabaseClient) GetRunningJobs() ([]Job, error) {
	query := `
		SELECT id, mode, gov_ids, date_string, begin_date, end_date, status,
			   result, error_message, created_at, started_at, completed_at, worker_id,
			   state, jurisdiction_name, case_name, blake2b_hash, case_data,
			   operation_type, limit_param, offset_param
		FROM jobs 
		WHERE status = 'running'
		ORDER BY started_at ASC
	`
	
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query running jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0)
	for rows.Next() {
		job := Job{}
		var govIDs interface{}
		var caseData interface{}
		var startedAt sql.NullTime
		var completedAt sql.NullTime
		var workerID sql.NullString
		var dateString sql.NullString
		var beginDate sql.NullString
		var endDate sql.NullString
		var result interface{}
		var errorMsg sql.NullString
		var state sql.NullString
		var jurisdictionName sql.NullString
		var caseName sql.NullString
		var blake2bHash sql.NullString
		var operationType sql.NullString
		var limitParam sql.NullInt64
		var offsetParam sql.NullInt64
		
		err := rows.Scan(
			&job.ID, &job.Mode, &govIDs, &dateString, &beginDate, &endDate,
			&job.Status, &result, &errorMsg, &job.CreatedAt, &startedAt,
			&completedAt, &workerID, &state, &jurisdictionName,
			&caseName, &blake2bHash, &caseData, &operationType,
			&limitParam, &offsetParam,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan running job row: %w", err)
		}

		// Handle nullable fields
		if dateString.Valid {
			job.DateString = dateString.String
		}
		if beginDate.Valid {
			job.BeginDate = beginDate.String
		}
		if endDate.Valid {
			job.EndDate = endDate.String
		}
		if result != nil {
			job.Result = result
		}
		if errorMsg.Valid {
			job.Error = errorMsg.String
		}
		if startedAt.Valid {
			job.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}
		if workerID.Valid {
			job.WorkerID = workerID.String
		}
		if state.Valid {
			job.State = state.String
		}
		if jurisdictionName.Valid {
			job.JurisdictionName = jurisdictionName.String
		}
		if caseName.Valid {
			job.CaseName = caseName.String
		}
		if blake2bHash.Valid {
			job.Blake2bHash = blake2bHash.String
		}
		if operationType.Valid {
			job.OperationType = operationType.String
		}
		if limitParam.Valid {
			job.Limit = int(limitParam.Int64)
		}
		if offsetParam.Valid {
			job.Offset = int(offsetParam.Int64)
		}

		// Handle array conversion for gov_ids
		if govIDs != nil {
			if govIDsSlice, ok := govIDs.([]interface{}); ok {
				job.GovIDs = make([]string, len(govIDsSlice))
				for i, v := range govIDsSlice {
					if str, ok := v.(string); ok {
						job.GovIDs[i] = str
					}
				}
			}
		}

		// Handle JSON conversion for case_data
		if caseData != nil {
			job.CaseData = caseData
		}

		jobs = append(jobs, job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating running job rows: %w", err)
	}

	return jobs, nil
}