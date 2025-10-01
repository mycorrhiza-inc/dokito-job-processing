'use client';

import { useEffect, useState } from 'react';
import { DashboardLayout } from '@/components/dashboard-layout';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { 
  Activity, 
  RefreshCw, 
  CheckCircle, 
  XCircle, 
  AlertCircle,
  Server,
  Database,
  Zap,
  Users
} from 'lucide-react';
import { api } from '@/lib/api';
import { HealthResponse } from '@/types/api';

export default function HealthPage() {
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  useEffect(() => {
    fetchHealthData();
    
    // Auto-refresh every 30 seconds
    const interval = setInterval(fetchHealthData, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchHealthData = async () => {
    try {
      const healthData = await api.getHealth();
      setHealth(healthData);
      setLastUpdate(new Date());
    } catch (error) {
      console.error('Failed to fetch health data:', error);
      setHealth(null);
    } finally {
      setLoading(false);
    }
  };

  const getHealthStatusIcon = (status: string) => {
    switch (status) {
      case 'healthy':
        return <CheckCircle className="h-5 w-5 text-green-500" />;
      case 'unhealthy':
        return <XCircle className="h-5 w-5 text-red-500" />;
      default:
        return <AlertCircle className="h-5 w-5 text-yellow-500" />;
    }
  };

  const getServiceIcon = (serviceName: string) => {
    switch (serviceName.toLowerCase()) {
      case 'kafka':
        return <Zap className="h-5 w-5" />;
      case 'database':
        return <Database className="h-5 w-5" />;
      case 'dokito':
      case 'dokito-backend':
        return <Server className="h-5 w-5" />;
      case 'workers':
      case 'worker':
        return <Users className="h-5 w-5" />;
      default:
        return <Activity className="h-5 w-5" />;
    }
  };

  const getServiceDescription = (serviceName: string) => {
    const descriptions: Record<string, string> = {
      'kafka': 'Message queue for job distribution and updates',
      'database': 'PostgreSQL database for persistent storage',
      'dokito': 'Rust processing monolith for data operations',
      'dokito-backend': 'Backend processing service',
      'api': 'Go REST API server',
      'worker': 'Node.js Playwright scraping workers',
      'workers': 'Node.js Playwright scraping workers',
      'zookeeper': 'Kafka coordination service',
      'nginx': 'Reverse proxy and load balancer'
    };
    
    return descriptions[serviceName.toLowerCase()] || 'System service';
  };

  if (loading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="text-lg text-gray-500">Loading system health...</div>
        </div>
      </DashboardLayout>
    );
  }

  const overallHealthy = health?.status === 'healthy';
  const healthyServices = health ? Object.values(health.services).filter(s => s === 'healthy').length : 0;
  const totalServices = health ? Object.keys(health.services).length : 0;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white flex items-center gap-3">
              <Activity className="h-8 w-8 text-green-600" />
              System Health
            </h1>
            <p className="mt-2 text-gray-600 dark:text-gray-300">
              Monitor the health and status of all system components
            </p>
            {lastUpdate && (
              <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                Last updated: {lastUpdate.toLocaleTimeString()}
              </p>
            )}
          </div>
          <Button onClick={fetchHealthData} variant="outline">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>

        {/* Overall Status */}
        <Card className={`border-2 ${overallHealthy ? 'border-green-200 dark:border-green-800' : 'border-red-200 dark:border-red-800'}`}>
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="flex items-center gap-3">
                {overallHealthy ? (
                  <CheckCircle className="h-8 w-8 text-green-500" />
                ) : (
                  <XCircle className="h-8 w-8 text-red-500" />
                )}
                System Status
              </CardTitle>
              <Badge 
                variant={overallHealthy ? 'default' : 'destructive'}
                className="text-lg px-4 py-2"
              >
                {health?.status?.toUpperCase() || 'UNKNOWN'}
              </Badge>
            </div>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="text-center">
                <div className="text-3xl font-bold text-green-600">
                  {healthyServices}
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  Healthy Services
                </div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-red-600">
                  {totalServices - healthyServices}
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  Unhealthy Services
                </div>
              </div>
              <div className="text-center">
                <div className="text-3xl font-bold text-blue-600">
                  {totalServices}
                </div>
                <div className="text-sm text-gray-600 dark:text-gray-400">
                  Total Services
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Services Status */}
        {health ? (
          <div className="grid gap-4">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Service Details
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {Object.entries(health.services).map(([serviceName, status]) => (
                <Card key={serviceName} className="relative">
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        {getServiceIcon(serviceName)}
                        <CardTitle className="text-lg capitalize">
                          {serviceName.replace('-', ' ')}
                        </CardTitle>
                      </div>
                      <div className="flex items-center gap-2">
                        {getHealthStatusIcon(status)}
                        <Badge variant={status === 'healthy' ? 'default' : 'destructive'}>
                          {status}
                        </Badge>
                      </div>
                    </div>
                  </CardHeader>
                  <CardContent>
                    <CardDescription className="text-sm">
                      {getServiceDescription(serviceName)}
                    </CardDescription>
                  </CardContent>
                  
                  {/* Status indicator */}
                  <div 
                    className={`absolute top-0 right-0 w-3 h-3 rounded-full ${
                      status === 'healthy' ? 'bg-green-500' : 'bg-red-500'
                    }`}
                  />
                </Card>
              ))}
            </div>
          </div>
        ) : (
          <Card>
            <CardContent className="py-12">
              <div className="text-center">
                <XCircle className="mx-auto h-12 w-12 text-red-500 mb-4" />
                <h3 className="text-lg font-medium text-gray-900 dark:text-white">
                  Health Check Failed
                </h3>
                <p className="text-gray-600 dark:text-gray-400 mt-2">
                  Unable to retrieve system health information. The API may be unavailable.
                </p>
                <div className="mt-4">
                  <Button onClick={fetchHealthData}>
                    <RefreshCw className="h-4 w-4 mr-2" />
                    Try Again
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Health Check Information */}
        <Card>
          <CardHeader>
            <CardTitle>Health Check Information</CardTitle>
            <CardDescription>
              Understanding the system health monitoring
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4 text-sm">
              <div>
                <h4 className="font-medium text-gray-900 dark:text-white">Health Check Frequency</h4>
                <p className="text-gray-600 dark:text-gray-400">
                  System health is checked every 30 seconds automatically, with manual refresh available.
                </p>
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900 dark:text-white">Service Monitoring</h4>
                <p className="text-gray-600 dark:text-gray-400">
                  Each service reports its own health status including connectivity, resource usage, and operational state.
                </p>
              </div>
              
              <div>
                <h4 className="font-medium text-gray-900 dark:text-white">Status Indicators</h4>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-2">
                  <div className="flex items-center gap-2">
                    <CheckCircle className="h-4 w-4 text-green-500" />
                    <span>Healthy - Service operating normally</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <XCircle className="h-4 w-4 text-red-500" />
                    <span>Unhealthy - Service has issues</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <AlertCircle className="h-4 w-4 text-yellow-500" />
                    <span>Unknown - Status undetermined</span>
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}