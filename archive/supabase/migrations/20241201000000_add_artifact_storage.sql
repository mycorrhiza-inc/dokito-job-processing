-- Add artifact storage fields to jobs table
-- This migration adds support for storing large job results in S3/external storage

-- Add artifact storage columns to jobs table
ALTER TABLE jobs 
ADD COLUMN IF NOT EXISTS artifact_storage_url VARCHAR(1000),
ADD COLUMN IF NOT EXISTS artifact_metadata JSONB;

-- Add index for efficient artifact queries
CREATE INDEX IF NOT EXISTS idx_jobs_artifact_storage 
ON jobs(artifact_storage_url) 
WHERE artifact_storage_url IS NOT NULL;

-- Add index on artifact metadata for queries
CREATE INDEX IF NOT EXISTS idx_jobs_artifact_metadata 
ON jobs USING GIN(artifact_metadata) 
WHERE artifact_metadata IS NOT NULL;

-- Add comment for documentation
COMMENT ON COLUMN jobs.artifact_storage_url IS 'URL to external storage location (Supabase Storage or S3) for large job results';
COMMENT ON COLUMN jobs.artifact_metadata IS 'Metadata about stored artifacts (size, compression, type, etc.)';

-- Update the cleanup function to handle artifacts
CREATE OR REPLACE FUNCTION cleanup_old_jobs_with_artifacts()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log artifacts that will be deleted (for cleanup script reference)
    INSERT INTO job_logs (id, job_id, timestamp, level, message, metadata)
    SELECT 
        'cleanup-' || id || '-' || EXTRACT(EPOCH FROM NOW()),
        id,
        NOW(),
        'info',
        'Job marked for deletion - artifact cleanup needed',
        jsonb_build_object(
            'artifact_url', artifact_storage_url,
            'cleanup_reason', 'job_retention_policy'
        )
    FROM jobs 
    WHERE created_at < NOW() - INTERVAL '30 days' 
    AND artifact_storage_url IS NOT NULL;
    
    -- Delete jobs older than 30 days
    -- Note: External artifacts should be cleaned up separately based on job_logs
    DELETE FROM jobs 
    WHERE created_at < NOW() - INTERVAL '30 days';
    
    -- Delete logs for jobs that no longer exist (handled by CASCADE, but just in case)
    DELETE FROM job_logs 
    WHERE job_id NOT IN (SELECT id FROM jobs);
END;
$$;

-- Create a view to show artifact storage statistics
CREATE OR REPLACE VIEW artifact_storage_statistics AS
SELECT 
    COUNT(*) as total_jobs_with_artifacts,
    COUNT(CASE WHEN artifact_metadata->>'compressed' = 'true' THEN 1 END) as compressed_artifacts,
    AVG((artifact_metadata->>'original_size')::bigint) as avg_original_size_bytes,
    AVG((artifact_metadata->>'stored_size')::bigint) as avg_stored_size_bytes,
    SUM((artifact_metadata->>'stored_size')::bigint) as total_stored_bytes,
    COUNT(DISTINCT artifact_metadata->>'type') as unique_artifact_types
FROM jobs
WHERE artifact_storage_url IS NOT NULL 
AND artifact_metadata IS NOT NULL;

-- Grant permissions for the new columns (adjust based on your setup)
-- GRANT SELECT, UPDATE ON jobs TO service_role;
-- GRANT SELECT ON artifact_storage_statistics TO service_role;