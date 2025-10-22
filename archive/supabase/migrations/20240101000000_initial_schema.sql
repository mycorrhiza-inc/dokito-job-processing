-- JobRunner Supabase Database Schema
-- Run this script in your Supabase SQL editor to create the necessary tables

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id VARCHAR(255) PRIMARY KEY,
    mode VARCHAR(100) NOT NULL,
    gov_ids TEXT[],
    date_string VARCHAR(50),
    begin_date VARCHAR(50),
    end_date VARCHAR(50),
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    result JSONB,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker_id VARCHAR(255),
    
    -- Dokito-specific fields
    state VARCHAR(100),
    jurisdiction_name VARCHAR(255),
    case_name VARCHAR(500),
    blake2b_hash VARCHAR(128),
    case_data JSONB,
    operation_type VARCHAR(100),
    limit_param INTEGER,
    offset_param INTEGER,
    
    -- Constraints
    CONSTRAINT jobs_status_check CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
);

-- Create job_logs table
CREATE TABLE IF NOT EXISTS job_logs (
    id VARCHAR(255) PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    worker_id VARCHAR(255),
    metadata JSONB
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created_at ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_mode ON jobs(mode);
CREATE INDEX IF NOT EXISTS idx_jobs_worker_id ON jobs(worker_id);
CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_logs_timestamp ON job_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_job_logs_level ON job_logs(level);

-- Create RLS (Row Level Security) policies if needed
-- Note: Adjust these policies based on your authentication requirements

-- Enable RLS on jobs table
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

-- Enable RLS on job_logs table  
ALTER TABLE job_logs ENABLE ROW LEVEL SECURITY;

-- Create a policy that allows all operations for now
-- You should customize this based on your authentication needs
CREATE POLICY "Allow all operations on jobs" ON jobs
    FOR ALL USING (true) WITH CHECK (true);

CREATE POLICY "Allow all operations on job_logs" ON job_logs
    FOR ALL USING (true) WITH CHECK (true);

-- Optional: Create a function to automatically delete old jobs and logs
CREATE OR REPLACE FUNCTION cleanup_old_jobs()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Delete jobs older than 30 days
    DELETE FROM jobs 
    WHERE created_at < NOW() - INTERVAL '30 days';
    
    -- Delete logs for jobs that no longer exist (this should be handled by CASCADE, but just in case)
    DELETE FROM job_logs 
    WHERE job_id NOT IN (SELECT id FROM jobs);
END;
$$;

-- Optional: Create a trigger to automatically update completed_at when status changes to completed/failed/cancelled
CREATE OR REPLACE FUNCTION update_job_completed_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status IN ('completed', 'failed', 'cancelled') AND OLD.status NOT IN ('completed', 'failed', 'cancelled') THEN
        NEW.completed_at = NOW();
    END IF;
    
    IF NEW.status = 'running' AND OLD.status = 'pending' AND NEW.started_at IS NULL THEN
        NEW.started_at = NOW();
    END IF;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER trigger_update_job_timestamps
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_job_completed_at();

-- Create a view for job statistics
CREATE OR REPLACE VIEW job_statistics AS
SELECT 
    COUNT(*) as total_jobs,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_jobs,
    COUNT(CASE WHEN status = 'running' THEN 1 END) as running_jobs,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs,
    COUNT(CASE WHEN status = 'cancelled' THEN 1 END) as cancelled_jobs,
    AVG(EXTRACT(EPOCH FROM (completed_at - started_at))) as avg_execution_time_seconds
FROM jobs
WHERE created_at > NOW() - INTERVAL '24 hours';

-- Grant necessary permissions (adjust based on your setup)
-- If you're using a service role, you might need to grant permissions
-- GRANT ALL ON jobs TO service_role;
-- GRANT ALL ON job_logs TO service_role;
-- GRANT ALL ON job_statistics TO service_role;