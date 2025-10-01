import {
  Job,
  JobListResponse,
  CreateJobRequest,
  LogsResponse,
  HealthResponse,
  QueueStatus,
  ApiError,
  AddLogRequest,
  LogEntry,
} from '@/types/api';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080/api/v1';

class ApiClientError extends Error {
  constructor(message: string, public status?: number) {
    super(message);
    this.name = 'ApiClientError';
  }
}

async function fetchApi<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;
  
  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      let errorMessage = `HTTP ${response.status}: ${response.statusText}`;
      try {
        const errorData: ApiError = await response.json();
        errorMessage = errorData.error || errorMessage;
      } catch {
        // Use default error message if JSON parsing fails
      }
      throw new ApiClientError(errorMessage, response.status);
    }

    return await response.json();
  } catch (error) {
    if (error instanceof ApiClientError) {
      throw error;
    }
    throw new ApiClientError(`Network error: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

export const api = {
  // Job management
  async createJob(request: CreateJobRequest): Promise<Job> {
    return fetchApi<Job>('/jobs', {
      method: 'POST',
      body: JSON.stringify(request),
    });
  },

  async getJobs(limit = 10, offset = 0): Promise<JobListResponse> {
    const response = await fetchApi<JobListResponse>(`/jobs?limit=${limit}&offset=${offset}`);
    return {
      ...response,
      jobs: response.jobs || []
    };
  },

  async getJob(id: string): Promise<Job> {
    return fetchApi<Job>(`/jobs/${id}`);
  },

  async deleteJob(id: string): Promise<{ message: string }> {
    return fetchApi<{ message: string }>(`/jobs/${id}`, {
      method: 'DELETE',
    });
  },

  // Job logs
  async getJobLogs(id: string, limit = 100, offset = 0): Promise<LogsResponse> {
    return fetchApi<LogsResponse>(`/jobs/${id}/logs?limit=${limit}&offset=${offset}`);
  },

  async addJobLog(id: string, request: AddLogRequest): Promise<LogEntry> {
    return fetchApi<LogEntry>(`/jobs/${id}/logs`, {
      method: 'POST',
      body: JSON.stringify(request),
    });
  },

  // System status
  async getHealth(): Promise<HealthResponse> {
    // Health endpoint is outside the /api/v1 group
    const url = `${API_BASE_URL.replace('/api/v1', '')}/health`;
    return fetch(url, {
      headers: { 'Content-Type': 'application/json' }
    }).then(res => res.json());
  },

  async getQueueStatus(): Promise<QueueStatus> {
    return fetchApi<QueueStatus>('/queue/status');
  },
};

export { ApiClientError };