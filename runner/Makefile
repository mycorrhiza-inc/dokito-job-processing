# JobRunner Standalone Makefile
.PHONY: help build-scraper build-runner run clean test docker-check

SCRAPER_IMAGE := jobrunner-worker:latest
RUNNER_BINARY := runner

## Help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## Docker Setup
docker-check: ## Check if Docker is running
	@echo "Checking Docker..."
	@docker version >/dev/null 2>&1 || (echo "❌ Docker is not running or not installed" && exit 1)
	@echo "✅ Docker is running"

## Building
build-scraper: docker-check ## Build the scraper container image
	@echo "Building scraper container image: $(SCRAPER_IMAGE)"
	@echo "Copying scraper-wrapper.js from worker directory..."
	@cp ../worker/scraper-wrapper.js ./scraper-wrapper.js
	@docker build -f Dockerfile.scraper -t $(SCRAPER_IMAGE) .
	@echo "✅ Scraper image built successfully: $(SCRAPER_IMAGE)"

build-runner: ## Build the Go runner binary
	@echo "Building Go runner binary..."
	@go mod tidy
	@go build -o $(RUNNER_BINARY) main.go
	@echo "✅ Runner binary built: $(RUNNER_BINARY)"

build: build-scraper build-runner ## Build both scraper image and runner binary

## Running
run: build ## Build and run the runner
	@echo "Starting JobRunner..."
	@./$(RUNNER_BINARY)

run-docker: build-scraper ## Run with Docker socket detection
	@echo "Running with Docker socket detection..."
	@export DOCKER_SOCKET_PATH=$$(make detect-docker-socket) && ./$(RUNNER_BINARY)

detect-docker-socket: ## Detect the Docker socket path
	@if [ -S "/var/run/docker.sock" ]; then \
		echo "unix:///var/run/docker.sock"; \
	elif [ -S "/Users/$(USER)/.docker/run/docker.sock" ]; then \
		echo "unix:///Users/$(USER)/.docker/run/docker.sock"; \
	elif [ -S "/home/linuxbrew/.linuxbrew/var/run/docker.sock" ]; then \
		echo "unix:///home/linuxbrew/.linuxbrew/var/run/docker.sock"; \
	else \
		echo "unix:///var/run/docker.sock"; \
	fi

## Testing
test-scraper: build-scraper ## Test the scraper container directly
	@echo "Testing scraper container..."
	@mkdir -p $(PWD)/test-output
	@docker run --rm \
		-v $(PWD)/test-output:/app/output \
		-e GOVIDS="25-01799" \
		-e MODE="meta" \
		-e JOB_ID="test-job" \
		-e WORKER_ID="test-worker" \
		$(SCRAPER_IMAGE)
	@echo "Test output:"
	@ls -la $(PWD)/test-output/
	@if [ -f $(PWD)/test-output/25-01799.json ]; then \
		echo "✅ Scraper test successful"; \
		echo "📄 Case metadata:"; \
		cat $(PWD)/test-output/25-01799.json; \
		echo ""; \
		echo "📋 Summary:"; \
		cat $(PWD)/test-output/summary.json; \
	else \
		echo "❌ Scraper test failed - no output files found"; \
		exit 1; \
	fi

test-runner: build-runner ## Test the runner binary compilation
	@echo "Testing runner binary..."
	@./$(RUNNER_BINARY) --help 2>/dev/null || echo "Runner binary compiled successfully"

test-integration: build-runner ## Run integration tests against real dokito backend
	@echo "Running integration tests against real dokito backend..."
	@echo "Checking if dokito backend is healthy..."
	@curl -f http://localhost:8123/health > /dev/null 2>&1 || (echo "❌ Dokito backend not running on localhost:8123. Start it with 'docker-compose up dokito-backend'" && exit 1)
	@echo "✅ Dokito backend is healthy"
	@echo "Running Go integration tests..."
	@cd $(PWD) && DOKITO_BACKEND_URL=http://localhost:8123 go test -v -run "TestDokito.*Integration|TestFullDokitoPipeline" -timeout 10m
	@echo "✅ Integration tests completed"

test-dokito-real: build-runner ## Run dokito-specific tests against real server
	@echo "Running dokito tests against REAL server..."
	@echo "Checking if dokito backend is healthy..."
	@curl -f http://localhost:8123/health > /dev/null 2>&1 || (echo "❌ Dokito backend not running on localhost:8123. Start it with 'docker-compose up dokito-backend'" && exit 1)
	@echo "✅ Dokito backend is healthy"
	@echo "Running Go dokito tests..."
	@cd $(PWD) && DOKITO_BACKEND_URL=http://localhost:8123 go test -v -run "TestDokito" -timeout 10m
	@echo "✅ Dokito tests completed"

test: test-scraper test-runner test-integration ## Run all tests including integration tests

## Cleanup
clean: ## Clean up built artifacts
	@echo "Cleaning up..."
	@rm -f $(RUNNER_BINARY)
	@rm -f scraper-wrapper.js
	@docker rmi $(SCRAPER_IMAGE) 2>/dev/null || true
	@rm -rf /tmp/jobrunner test-output
	@echo "✅ Cleanup complete"

clean-containers: ## Clean up any running containers
	@echo "Cleaning up running containers..."
	@docker ps -a --filter "ancestor=$(SCRAPER_IMAGE)" --format "{{.ID}}" | xargs -r docker rm -f
	@echo "✅ Container cleanup complete"

## Development
dev: build ## Build and run in development mode with verbose output
	@echo "Running in development mode..."
	@export DOCKER_SOCKET_PATH=$$(make detect-docker-socket) && \
	export LOG_LEVEL=debug && \
	./$(RUNNER_BINARY)

## Information
info: ## Show build information
	@echo "=== JobRunner Build Info ==="
	@echo "Scraper Image: $(SCRAPER_IMAGE)"
	@echo "Runner Binary: $(RUNNER_BINARY)"
	@echo "Docker Socket: $$(make detect-docker-socket)"
	@echo "Go Version: $$(go version)"
	@echo "Docker Version:"
	@docker version --format "  Client: {{.Client.Version}}, Server: {{.Server.Version}}" 2>/dev/null || echo "  Docker not available"

.DEFAULT_GOAL := help