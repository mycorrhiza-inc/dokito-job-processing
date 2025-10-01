'use client';

import { useEffect, useState } from 'react';
import { DashboardLayout } from '@/components/dashboard-layout';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Progress } from '@/components/ui/progress';
import { 
  RefreshCw, 
  Play, 
  Clock, 
  Eye, 
  Trash2,
  Activity,
  Pause
} from 'lucide-react';
import { api } from '@/lib/api';
import { Job } from '@/types/api';
import { getStatusColor, formatDate, formatDuration, getScrapingModeDescription } from '@/lib/utils';
import Link from 'next/link';

export default function RunningJobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchRunningJobs();
    
    // Auto-refresh every 5 seconds
    const interval = setInterval(fetchRunningJobs, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchRunningJobs = async () => {
    try {
      setLoading(true);
      const response = await api.getJobs(50, 0);
      // Filter for running and pending jobs
      const runningJobs = (response.jobs || []).filter(job => 
        job.status === 'running' || job.status === 'pending'
      );
      setJobs(runningJobs);
    } catch (error) {
      console.error('Failed to fetch running jobs:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleCancelJob = async (jobId: string) => {
    try {
      await api.deleteJob(jobId);
      setJobs(jobs.filter(job => job.id !== jobId));
    } catch (error) {
      console.error('Failed to cancel job:', error);
    }
  };

  const getJobProgress = (job: Job): number => {
    // This is a simple estimation based on job status
    // In a real implementation, you might have actual progress data
    if (job.status === 'pending') return 0;
    if (job.status === 'running') {
      // Estimate progress based on duration
      if (job.started_at) {
        const startTime = new Date(job.started_at).getTime();
        const now = Date.now();
        const elapsed = now - startTime;
        // Rough estimation: most jobs complete within 5 minutes
        const estimated = Math.min((elapsed / (5 * 60 * 1000)) * 100, 90);
        return estimated;
      }
      return 10; // Default for running without start time
    }
    return 100;
  };

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="text-lg text-gray-500">Loading running jobs...</div>
        </div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white flex items-center gap-3">
              <Play className="h-8 w-8 text-blue-600" />
              Running Jobs
            </h1>
            <p className="mt-2 text-gray-600 dark:text-gray-300">
              Monitor currently active and pending jobs
            </p>
          </div>
          <div className="flex gap-2">
            <Button onClick={fetchRunningJobs} variant="outline">
              <RefreshCw className="h-4 w-4 mr-2" />
              Refresh
            </Button>
            <Link href="/create">
              <Button>Create Job</Button>
            </Link>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Active Jobs</CardTitle>
              <Activity className="h-4 w-4 text-blue-600" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {jobs.filter(job => job.status === 'running').length}
              </div>
              <p className="text-xs text-gray-600 dark:text-gray-400">
                Currently executing
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Pending Jobs</CardTitle>
              <Clock className="h-4 w-4 text-yellow-600" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {jobs.filter(job => job.status === 'pending').length}
              </div>
              <p className="text-xs text-gray-600 dark:text-gray-400">
                Waiting to start
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Active</CardTitle>
              <Play className="h-4 w-4 text-green-600" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{jobs.length}</div>
              <p className="text-xs text-gray-600 dark:text-gray-400">
                All running + pending
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Running Jobs List */}
        {jobs.length > 0 ? (
          <div className="space-y-4">
            {jobs.map((job) => (
              <Card key={job.id}>
                <CardContent className="pt-6">
                  <div className="flex items-start justify-between">
                    <div className="flex-1 space-y-3">
                      {/* Header */}
                      <div className="flex items-center gap-3">
                        <Badge className={getStatusColor(job.status)}>
                          {job.status}
                        </Badge>
                        <span className="font-medium text-lg">{job.mode}</span>
                        <span className="text-sm text-gray-500 font-mono">
                          {job.id.slice(0, 8)}...
                        </span>
                      </div>

                      {/* Description */}
                      <p className="text-sm text-gray-600 dark:text-gray-400">
                        {getScrapingModeDescription(job.mode)}
                      </p>

                      {/* Progress Bar */}
                      {job.status === 'running' && (
                        <div className="space-y-2">
                          <div className="flex justify-between text-sm">
                            <span className="text-gray-600 dark:text-gray-400">Progress</span>
                            <span className="text-gray-600 dark:text-gray-400">
                              {Math.round(getJobProgress(job))}%
                            </span>
                          </div>
                          <Progress value={getJobProgress(job)} className="h-2" />
                        </div>
                      )}

                      {/* Job Details */}
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Created:</span>
                          <div className="font-medium">{formatDate(job.created_at)}</div>
                        </div>
                        
                        {job.started_at && (
                          <div>
                            <span className="text-gray-500 dark:text-gray-400">Duration:</span>
                            <div className="font-medium">
                              {formatDuration(job.started_at)}
                            </div>
                          </div>
                        )}
                        
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Worker:</span>
                          <div className="font-medium font-mono text-xs">
                            {job.worker_id || 'Unassigned'}
                          </div>
                        </div>

                        {/* Job Parameters */}
                        <div>
                          <span className="text-gray-500 dark:text-gray-400">Parameters:</span>
                          <div className="font-medium text-xs">
                            {job.gov_ids && job.gov_ids.length > 0 && (
                              <span>IDs: {job.gov_ids.length}</span>
                            )}
                            {job.date_string && (
                              <span>Date: {job.date_string}</span>
                            )}
                            {job.state && (
                              <span>{job.state}/{job.jurisdiction_name}</span>
                            )}
                            {!job.gov_ids && !job.date_string && !job.state && (
                              <span>None</span>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="flex items-center gap-2 ml-4">
                      <Link href={`/jobs/${job.id}`}>
                        <Button variant="outline" size="sm">
                          <Eye className="h-4 w-4" />
                        </Button>
                      </Link>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleCancelJob(job.id)}
                        className="text-red-600 hover:text-red-700"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        ) : (
          <Card>
            <CardContent className="py-12">
              <div className="text-center">
                <Pause className="mx-auto h-12 w-12 text-gray-400 mb-4" />
                <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                  No Running Jobs
                </h3>
                <p className="text-gray-600 dark:text-gray-400 mt-2">
                  All jobs have completed or there are no active jobs at the moment.
                </p>
                <div className="mt-4">
                  <Link href="/create">
                    <Button>
                      <Play className="h-4 w-4 mr-2" />
                      Create New Job
                    </Button>
                  </Link>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  );
}