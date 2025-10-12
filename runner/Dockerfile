# Multi-stage Dockerfile for JobRunner
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies for building
RUN apk add --no-cache git ca-certificates tzdata

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY *.go ./

# Build the runner binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o runner .

# Final runtime stage
FROM alpine:latest

# Install Docker CLI for container management
RUN apk add --no-cache ca-certificates curl docker-cli

WORKDIR /app

# Copy the runner binary from builder stage
COPY --from=builder /app/runner .

# Create a non-root user
RUN adduser -D -s /bin/sh runner
RUN chown runner:runner /app/runner

# Set environment variables
ENV DOCKER_HOST=unix:///var/run/docker.sock
ENV LOG_LEVEL=info

# Switch to non-root user
USER runner

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD pgrep -f "./runner" > /dev/null || exit 1

# Default command
CMD ["./runner"]