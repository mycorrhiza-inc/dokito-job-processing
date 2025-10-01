'use client';

import { useEffect, useState } from 'react';

import { useParams } from 'next/navigation';
import { DashboardLayout } from '@/components/dashboard-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { 
  ArrowLeft, 
  RefreshCw, 
  Download, 
  Trash2, 
  Clock,
  Calendar,
  User,
  Settings,
  FileText,
  AlertCircle
} from 'lucide-react';
import { api } from '@/lib/api';
import { Job, LogEntry, LogLevel } from '@/types/api';
import { getStatusColor, formatDate, formatDuration, getScrapingModeDescription } from '@/lib/utils';
import Link from 'next/link';
import { useRouter } from 'next/navigation';

export default function JobDetailsPage() {
  const params = useParams();
  const router = useRouter();
  const jobId = params.id as string;
  
  const [job, setJob] = useState<Job | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [logsLoading, setLogsLoading] = useState(false);

  useEffect(() => {
    if (jobId) {
      fetchJobDetails();
      fetchJobLogs();
      
      // Set up real-time updates for active jobs
      const interval = setInterval(() => {
        if (job && (job.status === 'running' || job.status === 'pending')) {
          fetchJobDetails();
          fetchJobLogs();
        }
      }, 5000); // Update every 5 seconds for running/pending jobs

      return () => clearInterval(interval);
    }
  }, [jobId, job?.status]);

  const fetchJobDetails = async () => {
    try {
      const jobData = await api.getJob(jobId);
      setJob(jobData);
    } catch (error) {
      console.error('Failed to fetch job details:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchJobLogs = async () => {
    try {
      setLogsLoading(true);
      const logsData = await api.getJobLogs(jobId, 1000, 0);
      setLogs(logsData.logs.reverse()); // Show newest logs first
    } catch (error) {
      console.error('Failed to fetch job logs:', error);
    } finally {
      setLogsLoading(false);
    }
  };

  const handleDeleteJob = async () => {
    if (job && confirm('Are you sure you want to delete this job?')) {
      try {
        await api.deleteJob(job.id);
        router.push('/jobs');
      } catch (error) {
        console.error('Failed to delete job:', error);
      }
    }
  };

  const getLogLevelColor = (level: LogLevel) => {
    switch (level) {
      case 'debug':
        return 'text-gray-500';
      case 'info':
        return 'text-blue-600';
      case 'warn':
        return 'text-yellow-600';
      case 'error':
        return 'text-red-600';
      default:
        return 'text-gray-500';
    }
  };

  const downloadLogs = () => {
    if (logs.length === 0) return;
    
    const logsText = logs.map(log => 
      `[${new Date(log.timestamp).toISOString()}] ${log.level.toUpperCase()}: ${log.message}`
    ).join('\n');
    
    const blob = new Blob([logsText], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `job-${jobId}-logs.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="text-lg text-gray-500">Loading job details...</div>
        </div>
      </DashboardLayout>
    );
  }

  if (!job) {
    return (
      <DashboardLayout>
        <div className="text-center py-12">
          <AlertCircle className="mx-auto h-12 w-12 text-gray-400 mb-4" />
          <h2 className="text-lg font-medium text-gray-900 dark:text-white">Job not found</h2>
          <p className="text-gray-600 dark:text-gray-400 mt-2">
            The job you&apos;re looking for doesn&apos;t exist or has been deleted.
          </p>
          <Link href="/jobs">
            <Button className="mt-4">Back to Jobs</Button>
          </Link>
        </div>
      </DashboardLayout>
    );
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/jobs">
              <Button variant="outline" size="sm">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back
              </Button>
            </Link>
            <div>
              <h1 className="text-3xl font-bold text-gray-900 dark:text-white">
                Job Details
              </h1>
              <p className="text-gray-600 dark:text-gray-300 font-mono">
                {job.id}
              </p>
            </div>
          </div>
          <div className="flex gap-2">
            <Button onClick={fetchJobDetails} variant="outline">
              <RefreshCw className="h-4 w-4 mr-2" />
              Refresh
            </Button>
            <Button onClick={handleDeleteJob} variant="outline" className="text-red-600 hover:text-red-700">
              <Trash2 className="h-4 w-4 mr-2" />
              Delete
            </Button>
          </div>
        </div>

        {/* Job Summary */}
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Settings className="h-5 w-5" />
                Job Summary
              </CardTitle>
              <Badge className={getStatusColor(job.status)}>
                {job.status}
              </Badge>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <div>
                <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 mb-1">
                  <Settings className="h-4 w-4" />
                  Mode
                </div>
                <div className="font-medium">{job.mode}</div>
                <div className="text-sm text-gray-600 dark:text-gray-300">
                  {getScrapingModeDescription(job.mode)}
                </div>
              </div>
              
              <div>
                <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 mb-1">
                  <Calendar className="h-4 w-4" />
                  Created
                </div>
                <div className="font-medium">{formatDate(job.created_at)}</div>
                {job.started_at && (
                  <div className="text-sm text-gray-600 dark:text-gray-300">
                    Started: {formatDate(job.started_at)}
                  </div>
                )}
              </div>
              
              <div>
                <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 mb-1">
                  <Clock className="h-4 w-4" />
                  Duration
                </div>
                <div className="font-medium">
                  {job.started_at ? formatDuration(job.started_at, job.completed_at) : 'Not started'}
                </div>
              </div>
              
              <div>
                <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400 mb-1">
                  <User className="h-4 w-4" />
                  Worker
                </div>
                <div className="font-medium font-mono">
                  {job.worker_id || 'Unassigned'}
                </div>
              </div>
            </div>

            {/* Job Parameters */}
            {(job.gov_ids || job.date_string || job.begin_date || job.state) && (
              <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
                <h4 className="font-medium mb-3">Parameters</h4>
                <div className="space-y-2">
                  {job.gov_ids && job.gov_ids.length > 0 && (
                    <div>
                      <span className="text-sm text-gray-500 dark:text-gray-400">Gov IDs: </span>
                      <span className="font-mono text-sm">{job.gov_ids.join(', ')}</span>
                    </div>
                  )}
                  {job.date_string && (
                    <div>
                      <span className="text-sm text-gray-500 dark:text-gray-400">Date: </span>
                      <span className="font-mono text-sm">{job.date_string}</span>
                    </div>
                  )}
                  {job.begin_date && job.end_date && (
                    <div>
                      <span className="text-sm text-gray-500 dark:text-gray-400">Date Range: </span>
                      <span className="font-mono text-sm">{job.begin_date} - {job.end_date}</span>
                    </div>
                  )}
                  {job.state && (
                    <div>
                      <span className="text-sm text-gray-500 dark:text-gray-400">State: </span>
                      <span className="font-mono text-sm">{job.state}</span>
                    </div>
                  )}
                  {job.jurisdiction_name && (
                    <div>
                      <span className="text-sm text-gray-500 dark:text-gray-400">Jurisdiction: </span>
                      <span className="font-mono text-sm">{job.jurisdiction_name}</span>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Error Message */}
            {job.error && (
              <div className="mt-6 pt-6 border-t border-gray-200 dark:border-gray-700">
                <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
                  <div className="flex items-center gap-2 text-red-800 dark:text-red-200 mb-2">
                    <AlertCircle className="h-4 w-4" />
                    <span className="font-medium">Error</span>
                  </div>
                  <div className="text-red-700 dark:text-red-300 text-sm font-mono">
                    {job.error}
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Tabs */}
        <Tabs defaultValue="logs" className="space-y-4">
          <TabsList>
            <TabsTrigger value="logs" className="flex items-center gap-2">
              <FileText className="h-4 w-4" />
              Logs ({logs.length})
            </TabsTrigger>
            {job.result != null && (
              <TabsTrigger value="result">Result</TabsTrigger>
            )}
          </TabsList>

          <TabsContent value="logs" className="space-y-4">
            <Card>
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Job Logs</CardTitle>
                    <CardDescription>
                      Real-time logs from job execution
                      {job.status === 'running' && (
                        <span className="ml-2 text-blue-600">(Auto-refreshing)</span>
                      )}
                    </CardDescription>
                  </div>
                  <div className="flex gap-2">
                    <Button onClick={fetchJobLogs} variant="outline" size="sm" disabled={logsLoading}>
                      <RefreshCw className={`h-4 w-4 mr-2 ${logsLoading ? 'animate-spin' : ''}`} />
                      Refresh
                    </Button>
                    <Button onClick={downloadLogs} variant="outline" size="sm" disabled={logs.length === 0}>
                      <Download className="h-4 w-4 mr-2" />
                      Download
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="bg-gray-900 dark:bg-gray-950 rounded-lg p-4 max-h-96 overflow-y-auto">
                  {logs.length > 0 ? (
                    <div className="space-y-1 font-mono text-sm">
                      {logs.map((log, index) => (
                        <div key={log.id || `${log.timestamp}-${index}`} className="flex gap-3">
                          <span className="text-gray-400 text-xs min-w-[140px]">
                            {new Date(log.timestamp).toLocaleTimeString()}
                          </span>
                          <span className={`text-xs min-w-[50px] font-medium uppercase ${getLogLevelColor(log.level)}`}>
                            {log.level}
                          </span>
                          <span className="text-gray-300 flex-1">
                            {log.message}
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-center text-gray-400 py-8">
                      No logs available for this job
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {job.result != null && (
            <TabsContent value="result">
              <Card>
                <CardHeader>
                  <CardTitle>Job Result</CardTitle>
                  <CardDescription>Output data from job execution</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="bg-gray-50 dark:bg-gray-800 rounded-lg p-4">
                    <pre className="text-sm overflow-x-auto">
                      {JSON.stringify(job.result, null, 2)}
                    </pre>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          )}
        </Tabs>
      </div>
    </DashboardLayout>
  );
}