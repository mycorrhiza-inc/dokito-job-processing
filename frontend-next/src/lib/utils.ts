import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"
import { JobStatus, ScrapingMode } from '@/types/api'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(dateString: string): string {
  return new Date(dateString).toLocaleString()
}

export function formatDuration(startTime: string, endTime?: string): string {
  const start = new Date(startTime)
  const end = endTime ? new Date(endTime) : new Date()
  const diffMs = end.getTime() - start.getTime()
  
  const seconds = Math.floor(diffMs / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m`
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`
  } else {
    return `${seconds}s`
  }
}

export function getStatusColor(status: JobStatus): string {
  switch (status) {
    case 'pending':
      return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-300'
    case 'running':
      return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300'
    case 'completed':
      return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300'
    case 'failed':
      return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300'
    case 'cancelled':
      return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300'
    default:
      return 'bg-gray-100 text-gray-800 dark:bg-gray-800 dark:text-gray-300'
  }
}

export function getScrapingModeDescription(mode: ScrapingMode): string {
  const descriptions: Record<ScrapingMode, string> = {
    'full': 'Complete case scraping with all data',
    'meta': 'Metadata only extraction',
    'docs': 'Documents only scraping',
    'parties': 'Parties information only',
    'dates': 'Cases by specific date',
    'full-extraction': 'Enhanced full extraction',
    'case-list': 'Complete cases catalog',
    'filings-between-dates': 'Filings in date range',
    'dokito-case-fetch': 'Fetch processed case data',
    'dokito-caselist': 'List jurisdiction cases',
    'dokito-attachment-obj': 'Fetch attachment metadata',
    'dokito-attachment-raw': 'Fetch raw attachment file',
    'dokito-case-submit': 'Submit case for processing',
    'dokito-reprocess': 'Trigger reprocessing operations'
  }
  
  return descriptions[mode] || 'Unknown scraping mode'
}

export function isRunningJob(status: JobStatus): boolean {
  return status === 'running' || status === 'pending'
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export function truncateText(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text
  return text.substring(0, maxLength) + '...'
}