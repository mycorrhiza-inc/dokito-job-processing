# Development stage
FROM mcr.microsoft.com/playwright:v1.54.1-noble AS development

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install all dependencies including dev dependencies
RUN npm ci

# Copy worker source code
COPY . .

# Create downloads directory
RUN mkdir -p /app/downloads

# Set environment variables for development
ENV NODE_ENV=development
ENV WORKER_CONCURRENCY=1

# Use existing ubuntu user (UID 1000) for consistency
RUN chown -R ubuntu:ubuntu /app
USER ubuntu

# Start the worker with nodemon for hot reload
CMD ["npx", "nodemon", "index.js"]

# Production stage (default)
FROM mcr.microsoft.com/playwright:v1.54.1-noble

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --only=production

# Copy worker source code
COPY . .

# Create downloads directory
RUN mkdir -p /app/downloads

# Set environment variables
ENV NODE_ENV=production
ENV WORKER_CONCURRENCY=3

# Use existing ubuntu user (UID 1000) for consistency
RUN chown -R ubuntu:ubuntu /app
USER ubuntu

# Start the worker
CMD ["node", "index.js"]