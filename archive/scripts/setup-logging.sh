#!/bin/bash

# Setup centralized logging directory structure
LOGS_DIR="/Users/orchid/mch/dockito/jobrunner/output/logs"

echo "Setting up centralized logging structure..."

# Create main logs directory
mkdir -p "$LOGS_DIR"

# Create dokito-backend logs directory
mkdir -p "$LOGS_DIR/dokito-backend"

# Create a sample pipeline logs directory to demonstrate structure
mkdir -p "$LOGS_DIR/sample-pipeline-id"

echo "Directory structure created:"
echo "├── $LOGS_DIR/"
echo "│   ├── dokito-backend/"
echo "│   └── {pipelineID}/"
echo "│       ├── log.txt"
echo "│       └── scraper.txt"

echo ""
echo "Logging setup complete!"
echo "Pipeline logs will be written to: $LOGS_DIR/{pipelineID}/"
echo "Dokito backend logs will be written to: $LOGS_DIR/dokito-backend/"