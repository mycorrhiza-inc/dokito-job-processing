export type JobStatus = 
  | 'pending' 
  | 'running' 
  | 'completed' 
  | 'failed' 
  | 'cancelled';

export type ScrapingMode =
  // Playwright scraping modes
  | 'full'
  | 'meta'
  | 'docs'
  | 'parties'
  | 'dates'
  | 'full-extraction'
  | 'filings-between-dates'
  | 'case-list'
  // Dokito backend API modes
  | 'dokito-case-fetch'
  | 'dokito-caselist'
  | 'dokito-attachment-obj'
  | 'dokito-attachment-raw'
  | 'dokito-case-submit'
  | 'dokito-reprocess';

export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  message: string;
  worker_id?: string;
  metadata?: Record<string, unknown>;
}

export interface Job {
  id: string;
  mode: ScrapingMode;
  gov_ids?: string[];
  date_string?: string;
  begin_date?: string;
  end_date?: string;
  status: JobStatus;
  result?: unknown;
  error?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  worker_id?: string;
  
  // Dokito-specific parameters
  state?: string;
  jurisdiction_name?: string;
  case_name?: string;
  blake2b_hash?: string;
  case_data?: unknown;
  operation_type?: string;
  limit?: number;
  offset?: number;
}

export interface CreateJobRequest {
  mode: ScrapingMode;
  gov_ids?: string[];
  date_string?: string;
  begin_date?: string;
  end_date?: string;
  
  // Dokito-specific parameters
  state?: string;
  jurisdiction_name?: string;
  case_name?: string;
  blake2b_hash?: string;
  case_data?: unknown;
  operation_type?: string;
  limit?: number;
  offset?: number;
}

export interface JobListResponse {
  jobs: Job[];
  total: number;
}

export interface LogsResponse {
  logs: LogEntry[];
  total: number;
}

export interface HealthResponse {
  status: 'healthy' | 'unhealthy';
  timestamp: string;
  services: Record<string, string>;
}

export interface ApiError {
  error: string;
}

export interface QueueStatus {
  message: string;
  timestamp: string;
}

export interface AddLogRequest {
  level: LogLevel;
  message: string;
  worker_id?: string;
  metadata?: Record<string, unknown>;
}