# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main .

# Development stage
FROM golang:1.25-alpine AS development

WORKDIR /app

# Install git for go modules and air for hot reload
RUN apk add --no-cache git
RUN go install github.com/air-verse/air@latest

# Create a non-root user with same UID as worker container
RUN adduser -D -u 1000 apiuser

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Create logs directory and set ownership
RUN mkdir -p /shared/logs && chown -R apiuser:apiuser /app /shared/logs

# Switch to non-root user
USER apiuser

# Expose port and debugger port
EXPOSE 8080 2345

# Set environment variables for development
ENV GIN_MODE=debug

# Use air for hot reloading in development
CMD ["air"]

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create a non-root user with same UID as worker container
RUN adduser -D -u 1000 apiuser

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/main .

# Create logs directory and set ownership
RUN mkdir -p /shared/logs && chown -R apiuser:apiuser /app /shared/logs

# Switch to non-root user
USER apiuser

# Expose port
EXPOSE 8080

# Set environment variables
ENV GIN_MODE=release

# Run the binary
CMD ["./main"]