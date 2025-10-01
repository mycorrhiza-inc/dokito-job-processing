# JobRunner Frontend

A modern Next.js dashboard for the JobRunner system built with shadcn/ui, providing comprehensive job management and system monitoring capabilities.

## Features

### 🎯 Core Functionality
- **Real-time Dashboard**: System overview with health metrics and job statistics
- **Job Management**: Complete CRUD operations for scraping jobs
- **Live Monitoring**: Real-time job status updates and progress tracking
- **Detailed Logs**: Streaming job logs with search and filtering
- **System Health**: Comprehensive health monitoring for all services

### 🎨 User Experience
- **Dark/Light Mode**: Automatic theme detection with manual toggle
- **Responsive Design**: Optimized for desktop, tablet, and mobile
- **Modern UI**: Built with shadcn/ui components and Tailwind CSS
- **Intuitive Navigation**: Clean sidebar navigation with active state indicators

### 🔧 Technical Features
- **TypeScript**: Full type safety with comprehensive API interfaces
- **Standalone Build**: Optimized for Docker deployment
- **Health Checks**: Built-in health endpoints for container orchestration
- **Real-time Updates**: Auto-refreshing data for running jobs and system status

## Quick Start

### With Docker Compose (Recommended)
```bash
# Start all services including new frontend
docker-compose up -d

# Access the dashboard
open http://localhost:8333
```

### Development Mode
```bash
cd frontend-next
npm install
npm run dev
# Access at http://localhost:3000
```

## Pages Overview

- **Dashboard** (`/`): System overview and recent activity
- **Jobs** (`/jobs`): Complete job management with filtering
- **Job Details** (`/jobs/[id]`): Detailed view with streaming logs
- **Create Job** (`/create`): Form-based job creation
- **Running Jobs** (`/running`): Live monitoring of active jobs
- **System Health** (`/health`): Service status and monitoring

## Docker Integration

The frontend is fully integrated with the JobRunner docker-compose system:

- **Port**: 3000 internal, 8333 external
- **Health Checks**: `/health.html` endpoint
- **Environment**: `NEXT_PUBLIC_API_URL` for API communication
- **Build**: Standalone Next.js output for optimal performance