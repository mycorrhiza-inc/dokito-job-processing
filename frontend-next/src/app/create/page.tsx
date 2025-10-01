'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { DashboardLayout } from '@/components/dashboard-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { 
  ArrowLeft, 
  Plus, 
  Info,
  AlertCircle,
  CheckCircle
} from 'lucide-react';
import { api } from '@/lib/api';
import { CreateJobRequest, ScrapingMode } from '@/types/api';
import { getScrapingModeDescription } from '@/lib/utils';
import Link from 'next/link';

interface JobFormState extends CreateJobRequest {
  gov_ids_input?: string; // For easier form handling
  case_data_input?: string; // For easier JSON form handling
}

const SCRAPING_MODES: { value: ScrapingMode; label: string; category: 'Playwright' | 'Dokito' }[] = [
  // Playwright modes
  { value: 'full', label: 'Full Scraping', category: 'Playwright' },
  { value: 'meta', label: 'Metadata Only', category: 'Playwright' },
  { value: 'docs', label: 'Documents Only', category: 'Playwright' },
  { value: 'parties', label: 'Parties Only', category: 'Playwright' },
  { value: 'dates', label: 'Cases by Date', category: 'Playwright' },
  { value: 'full-extraction', label: 'Enhanced Extraction', category: 'Playwright' },
  { value: 'case-list', label: 'Cases Catalog', category: 'Playwright' },
  { value: 'filings-between-dates', label: 'Date Range Filings', category: 'Playwright' },
  
  // Dokito modes
  { value: 'dokito-case-fetch', label: 'Case Fetch', category: 'Dokito' },
  { value: 'dokito-caselist', label: 'Case List', category: 'Dokito' },
  { value: 'dokito-attachment-obj', label: 'Attachment Metadata', category: 'Dokito' },
  { value: 'dokito-attachment-raw', label: 'Raw Attachment', category: 'Dokito' },
  { value: 'dokito-case-submit', label: 'Submit Case', category: 'Dokito' },
  { value: 'dokito-reprocess', label: 'Reprocess', category: 'Dokito' },
];

export default function CreateJobPage() {
  const router = useRouter();
  const [formData, setFormData] = useState<JobFormState>({
    mode: 'full',
    gov_ids_input: '',
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  const handleInputChange = (field: keyof JobFormState, value: string | number) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    setError(null);
  };

  const validateForm = (): string | null => {
    const { mode } = formData;

    switch (mode) {
      case 'full':
      case 'meta':
      case 'docs':
      case 'parties':
      case 'full-extraction':
        if (!formData.gov_ids_input?.trim()) {
          return 'Gov IDs are required for this mode';
        }
        break;
      
      case 'dates':
        if (!formData.date_string?.trim()) {
          return 'Date string is required for dates mode';
        }
        break;
      
      case 'filings-between-dates':
        if (!formData.begin_date || !formData.end_date) {
          return 'Both begin and end dates are required';
        }
        break;
      
      case 'case-list':
        // No additional parameters required
        break;
      
      case 'dokito-case-fetch':
        if (!formData.state || !formData.jurisdiction_name || !formData.case_name) {
          return 'State, jurisdiction name, and case name are required';
        }
        break;
      
      case 'dokito-caselist':
        if (!formData.state || !formData.jurisdiction_name) {
          return 'State and jurisdiction name are required';
        }
        break;
      
      case 'dokito-attachment-obj':
      case 'dokito-attachment-raw':
        if (!formData.blake2b_hash) {
          return 'Blake2b hash is required';
        }
        break;
      
      case 'dokito-case-submit':
        if (!formData.case_data_input?.trim()) {
          return 'Case data is required for case submit';
        }
        // Try to parse JSON to validate format
        try {
          JSON.parse(formData.case_data_input);
        } catch {
          return 'Case data must be valid JSON';
        }
        break;

      case 'dokito-reprocess':
        if (!formData.operation_type) {
          return 'Operation type is required';
        }
        // Purge operation requires state and jurisdiction
        if (formData.operation_type === 'purge') {
          if (!formData.state || !formData.jurisdiction_name) {
            return 'State and jurisdiction name are required for purge operation';
          }
        }
        break;
    }

    return null;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);

    const validationError = validateForm();
    if (validationError) {
      setError(validationError);
      return;
    }

    setLoading(true);

    try {
      const jobRequest: CreateJobRequest = {
        mode: formData.mode,
      };

      // Add mode-specific fields
      if (formData.gov_ids_input) {
        jobRequest.gov_ids = formData.gov_ids_input
          .split(',')
          .map(id => id.trim())
          .filter(id => id.length > 0);
      }

      if (formData.date_string) jobRequest.date_string = formData.date_string;
      if (formData.begin_date) jobRequest.begin_date = formData.begin_date;
      if (formData.end_date) jobRequest.end_date = formData.end_date;
      if (formData.state) jobRequest.state = formData.state;
      if (formData.jurisdiction_name) jobRequest.jurisdiction_name = formData.jurisdiction_name;
      if (formData.case_name) jobRequest.case_name = formData.case_name;
      if (formData.blake2b_hash) jobRequest.blake2b_hash = formData.blake2b_hash;
      if (formData.operation_type) jobRequest.operation_type = formData.operation_type;
      if (formData.limit) jobRequest.limit = formData.limit;
      if (formData.offset) jobRequest.offset = formData.offset;
      
      // Parse case_data from JSON string input
      if (formData.case_data_input) {
        try {
          jobRequest.case_data = JSON.parse(formData.case_data_input);
        } catch {
          setError('Invalid JSON format for case data');
          return;
        }
      }

      await api.createJob(jobRequest);
      setSuccess(true);
      setTimeout(() => {
        router.push('/jobs');
      }, 2000);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to create job');
    } finally {
      setLoading(false);
    }
  };

  const renderModeSpecificFields = () => {
    const { mode } = formData;

    switch (mode) {
      case 'full':
      case 'meta':
      case 'docs':
      case 'parties':
      case 'full-extraction':
        return (
          <div className="space-y-4">
            <div>
              <Label htmlFor="gov_ids">Government IDs</Label>
              <Input
                id="gov_ids"
                placeholder="Enter comma-separated IDs (e.g., 23-E-0238, 23-E-0239)"
                value={formData.gov_ids_input || ''}
                onChange={(e) => handleInputChange('gov_ids_input', e.target.value)}
              />
              <p className="text-sm text-gray-500 mt-1">
                Enter one or more government case IDs separated by commas
              </p>
            </div>
          </div>
        );

      case 'dates':
        return (
          <div className="space-y-4">
            <div>
              <Label htmlFor="date_string">Date String</Label>
              <Input
                id="date_string"
                placeholder="YYYY-MM-DD"
                value={formData.date_string || ''}
                onChange={(e) => handleInputChange('date_string', e.target.value)}
              />
              <p className="text-sm text-gray-500 mt-1">
                Date to scrape cases for
              </p>
            </div>
          </div>
        );

      case 'filings-between-dates':
        return (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="begin_date">Begin Date</Label>
                <Input
                  id="begin_date"
                  type="date"
                  value={formData.begin_date || ''}
                  onChange={(e) => handleInputChange('begin_date', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="end_date">End Date</Label>
                <Input
                  id="end_date"
                  type="date"
                  value={formData.end_date || ''}
                  onChange={(e) => handleInputChange('end_date', e.target.value)}
                />
              </div>
            </div>
          </div>
        );

      case 'dokito-case-fetch':
        return (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="state">State</Label>
                <Input
                  id="state"
                  placeholder="ny"
                  value={formData.state || ''}
                  onChange={(e) => handleInputChange('state', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="jurisdiction_name">Jurisdiction</Label>
                <Input
                  id="jurisdiction_name"
                  placeholder="psc"
                  value={formData.jurisdiction_name || ''}
                  onChange={(e) => handleInputChange('jurisdiction_name', e.target.value)}
                />
              </div>
            </div>
            <div>
              <Label htmlFor="case_name">Case Name</Label>
              <Input
                id="case_name"
                placeholder="23-E-0238"
                value={formData.case_name || ''}
                onChange={(e) => handleInputChange('case_name', e.target.value)}
              />
            </div>
          </div>
        );

      case 'dokito-caselist':
        return (
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="state">State</Label>
                <Input
                  id="state"
                  placeholder="ny"
                  value={formData.state || ''}
                  onChange={(e) => handleInputChange('state', e.target.value)}
                />
              </div>
              <div>
                <Label htmlFor="jurisdiction_name">Jurisdiction</Label>
                <Input
                  id="jurisdiction_name"
                  placeholder="psc"
                  value={formData.jurisdiction_name || ''}
                  onChange={(e) => handleInputChange('jurisdiction_name', e.target.value)}
                />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label htmlFor="limit">Limit</Label>
                <Input
                  id="limit"
                  type="number"
                  placeholder="50"
                  value={formData.limit || ''}
                  onChange={(e) => handleInputChange('limit', parseInt(e.target.value) || 0)}
                />
              </div>
              <div>
                <Label htmlFor="offset">Offset</Label>
                <Input
                  id="offset"
                  type="number"
                  placeholder="0"
                  value={formData.offset || ''}
                  onChange={(e) => handleInputChange('offset', parseInt(e.target.value) || 0)}
                />
              </div>
            </div>
          </div>
        );

      case 'dokito-attachment-obj':
      case 'dokito-attachment-raw':
        return (
          <div className="space-y-4">
            <div>
              <Label htmlFor="blake2b_hash">Blake2b Hash</Label>
              <Input
                id="blake2b_hash"
                placeholder="Enter Blake2b hash of the attachment"
                value={formData.blake2b_hash || ''}
                onChange={(e) => handleInputChange('blake2b_hash', e.target.value)}
              />
            </div>
          </div>
        );

      case 'dokito-case-submit':
        return (
          <div className="space-y-4">
            <div>
              <Label htmlFor="case_data">Case Data (JSON)</Label>
              <Textarea
                id="case_data"
                className="min-h-[200px]"
                placeholder="Enter case data as JSON object..."
                value={formData.case_data_input || ''}
                onChange={(e) => handleInputChange('case_data_input', e.target.value)}
              />
              <p className="text-sm text-gray-500 mt-1">
                Enter the complete case data as a valid JSON object
              </p>
            </div>
          </div>
        );

      case 'dokito-reprocess':
        return (
          <div className="space-y-4">
            <div>
              <Label htmlFor="operation_type">Operation Type</Label>
              <Select
                value={formData.operation_type || ''}
                onValueChange={(value) => handleInputChange('operation_type', value)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select operation" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="dockets">Reprocess Dockets</SelectItem>
                  <SelectItem value="hashes-random">Download Missing Hashes (Random)</SelectItem>
                  <SelectItem value="hashes-newest">Download Missing Hashes (Newest)</SelectItem>
                  <SelectItem value="purge">Purge Jurisdiction Data</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {/* Operation type descriptions */}
            {formData.operation_type && (
              <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
                <div className="flex items-center gap-2 text-blue-800 dark:text-blue-200">
                  <Info className="h-4 w-4" />
                  <span className="font-medium">Operation Description</span>
                </div>
                <p className="text-blue-700 dark:text-blue-300 text-sm mt-1">
                  {formData.operation_type === 'dockets' && 'Reprocess all dockets for all jurisdictions'}
                  {formData.operation_type === 'hashes-random' && 'Download missing attachment hashes in random order'}
                  {formData.operation_type === 'hashes-newest' && 'Download missing attachment hashes starting from newest'}
                  {formData.operation_type === 'purge' && 'Delete all data for the specified jurisdiction'}
                </p>
              </div>
            )}
            
            {formData.operation_type === 'purge' && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <Label htmlFor="state">State</Label>
                  <Input
                    id="state"
                    placeholder="ny"
                    value={formData.state || ''}
                    onChange={(e) => handleInputChange('state', e.target.value)}
                  />
                </div>
                <div>
                  <Label htmlFor="jurisdiction_name">Jurisdiction</Label>
                  <Input
                    id="jurisdiction_name"
                    placeholder="psc"
                    value={formData.jurisdiction_name || ''}
                    onChange={(e) => handleInputChange('jurisdiction_name', e.target.value)}
                  />
                </div>
              </div>
            )}
          </div>
        );

      case 'case-list':
        return (
          <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
            <div className="flex items-center gap-2 text-blue-800 dark:text-blue-200">
              <Info className="h-4 w-4" />
              <span className="font-medium">No parameters required</span>
            </div>
            <p className="text-blue-700 dark:text-blue-300 text-sm mt-1">
              This mode will scrape all available cases from the catalog.
            </p>
          </div>
        );

      default:
        return null;
    }
  };

  if (success) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <Card className="w-full max-w-md">
            <CardContent className="pt-6">
              <div className="text-center">
                <CheckCircle className="mx-auto h-12 w-12 text-green-500 mb-4" />
                <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                  Job Created Successfully
                </h3>
                <p className="text-gray-600 dark:text-gray-400 mt-2">
                  Redirecting to jobs page...
                </p>
              </div>
            </CardContent>
          </Card>
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
              <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Create Job</h1>
              <p className="mt-2 text-gray-600 dark:text-gray-300">
                Configure and submit a new scraping job
              </p>
            </div>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Mode Selection */}
            <div className="lg:col-span-2">
              <Card>
                <CardHeader>
                  <CardTitle>Job Configuration</CardTitle>
                  <CardDescription>
                    Select the scraping mode and configure parameters
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                  {/* Mode Selection */}
                  <div>
                    <Label htmlFor="mode">Scraping Mode</Label>
                    <Select
                      value={formData.mode}
                      onValueChange={(value) => handleInputChange('mode', value as ScrapingMode)}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <div className="px-2 py-1 text-xs font-medium text-gray-500 dark:text-gray-400">
                          Playwright Modes
                        </div>
                        {SCRAPING_MODES.filter(mode => mode.category === 'Playwright').map((mode) => (
                          <SelectItem key={mode.value} value={mode.value}>
                            {mode.label}
                          </SelectItem>
                        ))}
                        <div className="px-2 py-1 text-xs font-medium text-gray-500 dark:text-gray-400 border-t mt-2 pt-2">
                          Dokito Modes
                        </div>
                        {SCRAPING_MODES.filter(mode => mode.category === 'Dokito').map((mode) => (
                          <SelectItem key={mode.value} value={mode.value}>
                            {mode.label}
                          </SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Mode-specific fields */}
                  {renderModeSpecificFields()}
                </CardContent>
              </Card>
            </div>

            {/* Info Panel */}
            <div>
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Info className="h-5 w-5" />
                    Mode Info
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    <div>
                      <Badge variant="outline">
                        {SCRAPING_MODES.find(m => m.value === formData.mode)?.category}
                      </Badge>
                    </div>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      {getScrapingModeDescription(formData.mode)}
                    </p>
                  </div>
                </CardContent>
              </Card>
            </div>
          </div>

          {/* Error Display */}
          {error && (
            <div className="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg p-4">
              <div className="flex items-center gap-2 text-red-800 dark:text-red-200">
                <AlertCircle className="h-4 w-4" />
                <span className="font-medium">Error</span>
              </div>
              <p className="text-red-700 dark:text-red-300 text-sm mt-1">
                {error}
              </p>
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-end gap-4">
            <Link href="/jobs">
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </Link>
            <Button type="submit" disabled={loading}>
              {loading ? (
                <>
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white mr-2" />
                  Creating...
                </>
              ) : (
                <>
                  <Plus className="h-4 w-4 mr-2" />
                  Create Job
                </>
              )}
            </Button>
          </div>
        </form>
      </div>
    </DashboardLayout>
  );
}