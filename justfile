# JobRunner Development Environment
# Justfile for managing Supabase + Docker Compose integration

# Default recipe - show available commands
default:
    @just --list

# Start the complete development environment
dev: supabase-start docker-dev

# Start production environment (external Supabase)
prod: docker-prod

# === Supabase Management ===

# Initialize Supabase project (run once)
supabase-init:
    @echo "🚀 Initializing Supabase project..."
    supabase init
    @echo "✅ Supabase initialized successfully!"

# Start Supabase services and extract connection info
supabase-start:
    @echo "🚀 Starting Supabase services..."
    supabase start
    @echo "✅ Supabase started successfully!"
    @echo "📋 Extracting connection details..."
    @just supabase-env

# Stop Supabase services
supabase-stop:
    @echo "🛑 Stopping Supabase services..."
    supabase stop
    @echo "✅ Supabase stopped"

# Stop Supabase and reset all data
supabase-reset:
    @echo "⚠️  Stopping Supabase and resetting data..."
    supabase stop --no-backup
    @echo "✅ Supabase reset complete"

# Show Supabase service status
supabase-status:
    @supabase status

# Extract Supabase environment variables to .env.supabase
supabase-env:
    @echo "📝 Extracting Supabase environment variables..."
    @supabase status -o env > .env.supabase
    @echo "✅ Environment variables saved to .env.supabase"
    @cat .env.supabase

# === Docker Compose Management ===

# Start development environment with single Kafka (after Supabase is running)
docker-dev:
    @echo "🐳 Starting development services with single Kafka..."
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml  --env-file .env.supabase up -d
    @echo "✅ Development environment started!"
    @just show-services

# Start development environment with Kafka cluster
docker-dev-cluster *args:
    #!/usr/bin/env bash
    clean_build=false
    
    # Parse arguments
    for arg in {{args}}; do
        case "$arg" in
            --clean)
                clean_build=true
                ;;
            *)
                echo "❌ Unknown argument: $arg"
                echo "Usage: just docker-dev-cluster [--clean]"
                exit 1
                ;;
        esac
    done
    
    if [ "$clean_build" = true ]; then
        echo "🧹 Cleaning up existing containers and volumes..."
        docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.cluster.yml down -v
        docker system prune -f
        echo "✅ Cleanup complete"
        echo ""
    fi
    
    echo "🐳 Starting development services with Kafka cluster..."
    just _inject-supabase-env docker-compose.dev.yml
    
    if [ "$clean_build" = true ]; then
        echo "🏗️  Building from scratch..."
        docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.cluster.yml --env-file .env.supabase up -d --build --force-recreate
    else
        docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.cluster.yml --env-file .env.supabase up -d
    fi
    
    echo "⏳ Waiting for Kafka cluster to initialize..."
    sleep 60
    echo "🔧 Setting up Kafka cluster..."
    ./scripts/kafka-setup.sh setup
    echo "✅ Development environment with Kafka cluster started!"
    just show-services-cluster

# Start production environment with single Kafka
docker-prod:
    @echo "🐳 Starting production services with single Kafka..."
    docker-compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.single-kafka.yml --env-file .env up -d
    @echo "✅ Production environment started!"
    @just show-services-prod

# Start production environment with Kafka cluster
docker-prod-cluster:
    @echo "🐳 Starting production services with Kafka cluster..."
    docker-compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.cluster.yml --env-file .env up -d
    @echo "⏳ Waiting for Kafka cluster to initialize..."
    @sleep 60
    @echo "🔧 Setting up Kafka cluster..."
    @./scripts/kafka-setup.sh setup
    @echo "✅ Production environment with Kafka cluster started!"
    @just show-services-cluster

# Stop Docker services
docker-stop:
    @echo "🛑 Stopping Docker services..."
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml down
    docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
    @echo "✅ Docker services stopped"

# Show running services (development)
show-services:
    @echo "\n📊 Service Status (Development):"
    @echo "================================"
    @echo "🟢 Supabase Services:"
    @supabase status --output pretty | grep -E "(API URL|Studio URL|DB URL)" || echo "   Supabase not running"
    @echo ""
    @echo "🟢 Docker Services:"
    @docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.single-kafka.yml ps --format "table {{{{.Name}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}" 2>/dev/null || echo "   No Docker services running"
    @echo ""
    @echo "🌐 Service URLs:"
    @echo "   Frontend (dev): http://localhost:3333"
    @echo "   API:            http://localhost:8080"
    @echo "   Kafka UI:       http://localhost:8082 (debug profile)"
    @echo "   Supabase:       http://localhost:54323"

# Show running services with Kafka cluster
show-services-cluster:
    @echo "\n📊 Service Status (Kafka Cluster):"
    @echo "=================================="
    @echo "🟢 Supabase Services:"
    @supabase status --output pretty | grep -E "(API URL|Studio URL|DB URL)" || echo "   Supabase not running"
    @echo ""
    @echo "🟢 Docker Services:"
    @docker-compose -f docker-compose.yml -f docker-compose.dev.yml -f docker-compose.cluster.yml ps --format "table {{{{.Name}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}" 2>/dev/null || echo "   No Docker services running"
    @echo ""
    @echo "🌐 Service URLs:"
    @echo "   Frontend (dev): http://localhost:3333"
    @echo "   API:            http://localhost:8080"
    @echo "   Kafka UI:       http://localhost:8082"
    @echo "   Kafka Brokers:  localhost:9093,9095,9097"
    @echo "   Supabase:       http://localhost:54323"
    @echo ""
    @echo "⚡ Kafka Cluster Health:"
    @./scripts/kafka-setup.sh health || echo "   Kafka cluster health check failed"

# Show running services (production)
show-services-prod:
    @echo "\n📊 Service Status (Production):"
    @echo "==============================="
    @echo "🟢 Docker Services:"
    @docker-compose -f docker-compose.yml -f docker-compose.prod.yml ps --format "table {{{{.Name}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}" 2>/dev/null || echo "   No Docker services running"
    @echo ""
    @echo "🌐 Service URLs:"
    @echo "   Frontend:       http://localhost:8333"
    @echo "   API:            http://localhost:8080"
    @echo "   Nginx Proxy:    http://localhost:80"

# === Database Management ===

# Reset database with migrations and seed data
db-reset:
    @echo "🔄 Resetting database..."
    supabase db reset
    @echo "✅ Database reset complete"

# Create a new migration
db-migrate name:
    @echo "📝 Creating new migration: {{name}}"
    supabase migration new {{name}}
    @echo "✅ Migration created in supabase/migrations/"

# Generate TypeScript types from database schema
db-types:
    @echo "🔧 Generating TypeScript types..."
    supabase gen types typescript --local > types/supabase.ts
    @echo "✅ Types generated in types/supabase.ts"

# Pull schema changes from remote database
db-pull:
    @echo "📥 Pulling schema from remote database..."
    supabase db pull
    @echo "✅ Schema pulled successfully"

# Push local migrations to remote database
db-push:
    @echo "📤 Pushing migrations to remote database..."
    supabase db push
    @echo "✅ Migrations pushed successfully"

# === Environment Management ===

# Setup environment files
setup-env:
    @echo "📋 Setting up environment files..."
    @if [ ! -f .env ]; then cp .env.example .env; echo "✅ Created .env from .env.example"; else echo "📄 .env already exists"; fi
    @if [ -f .env.supabase ]; then echo "📄 .env.supabase exists"; else echo "⚠️  Run 'just supabase-start' to generate .env.supabase"; fi

# Show environment configuration
show-env:
    @echo "📋 Environment Configuration:"
    @echo "============================"
    @echo "Main config (.env):"
    @if [ -f .env ]; then grep -E "^[A-Z_]+=" .env | head -10; else echo "   .env not found - run 'just setup-env'"; fi
    @echo ""
    @echo "Supabase config (.env.supabase):"
    @if [ -f .env.supabase ]; then cat .env.supabase; else echo "   .env.supabase not found - run 'just supabase-start'"; fi

# === Utility Commands ===

# Clean up all services and data
clean: docker-stop supabase-stop
    @echo "🧹 Cleaning up containers and volumes..."
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml down -v
    docker-compose -f docker-compose.yml -f docker-compose.prod.yml down -v
    docker system prune -f
    @echo "✅ Cleanup complete"

# Full restart of the development environment
restart: stop dev

# Stop all services
stop: docker-stop supabase-stop

# Check system requirements
check:
    @echo "🔍 Checking system requirements..."
    @echo "Docker:"
    @docker --version || echo "❌ Docker not installed"
    @echo "Supabase CLI:"
    @supabase --version || echo "❌ Supabase CLI not installed - run: brew install supabase/tap/supabase"
    @echo "Just:"
    @just --version || echo "❌ Just not installed"
    @echo ""
    @if command -v docker >/dev/null && command -v supabase >/dev/null; then echo "✅ All requirements met!"; else echo "❌ Missing requirements - see above"; fi

# Show logs from all services
logs service="":
    #!/usr/bin/env bash
    if [ -z "{{service}}" ]; then
        echo "📋 Available services:"
        echo "Supabase: auth, db, rest, realtime, storage, studio"
        echo "Docker: api, worker, frontend, zookeeper, kafka, dokito-backend"
        echo ""
        echo "Usage: just logs <service>"
    else
        case "{{service}}" in
            auth|db|rest|realtime|storage|studio)
                echo "📋 Supabase {{service}} logs:"
                docker logs supabase_{{service}}_jobrunner --tail=50 -f 2>/dev/null || docker logs supabase-{{service}} --tail=50 -f
                ;;
            api|worker|frontend|zookeeper|kafka|dokito-backend)
                echo "📋 Docker {{service}} logs:"
                docker-compose -f docker-compose.yml -f docker-compose.dev.yml logs {{service}} --tail=50 -f
                ;;
            *)
                echo "❌ Unknown service: {{service}}"
                just logs
                ;;
        esac
    fi

# === Testing Commands ===

# Test connection to all services
test-connection:
    #!/usr/bin/env bash
    echo "🧪 Testing service connections..."
    
    # Test Supabase
    if curl -s http://localhost:54321/health >/dev/null; then
        echo "✅ Supabase API: OK"
    else
        echo "❌ Supabase API: Failed"
    fi
    
    # Test API
    if curl -s http://localhost:8080/health >/dev/null; then
        echo "✅ JobRunner API: OK"
    else
        echo "❌ JobRunner API: Failed"
    fi
    
    # Test Frontend (dev)
    if curl -s http://localhost:3333 >/dev/null; then
        echo "✅ Frontend (dev): OK"
    else
        echo "❌ Frontend (dev): Failed"
    fi
    
    # Test Dokito Backend
    if curl -s http://localhost:8123/health >/dev/null; then
        echo "✅ Dokito Backend: OK"
    else
        echo "❌ Dokito Backend: Failed"
    fi
    
    # Test Kafka UI (if debug profile is running)
    if curl -s http://localhost:8082 >/dev/null; then
        echo "✅ Kafka UI: OK"
    else
        echo "❌ Kafka UI: Not running or failed"
    fi

# Run database tests
test-db:
    @echo "🧪 Running database tests..."
    supabase test db

# === Internal Helper Commands ===

# Internal: Inject Supabase environment into Docker Compose
_inject-supabase-env compose_file:
    @echo "🔧 Injecting Supabase environment into {{compose_file}}..."
    @test -f .env.supabase || (echo "❌ .env.supabase not found. Run 'just supabase-start' first." && exit 1)
    @supabase status -o env > .env.supabase.tmp
    @echo "" >> .env.supabase.tmp
    @echo "# Mapped variables for Docker Compose compatibility" >> .env.supabase.tmp
    @grep "API_URL=" .env.supabase.tmp | sed 's/API_URL=/SUPABASE_URL=/' >> .env.supabase.tmp
    @grep "ANON_KEY=" .env.supabase.tmp | sed 's/ANON_KEY=/SUPABASE_ANON_KEY=/' >> .env.supabase.tmp
    @grep "SERVICE_ROLE_KEY=" .env.supabase.tmp | sed 's/SERVICE_ROLE_KEY=/SUPABASE_SERVICE_ROLE_KEY=/' >> .env.supabase.tmp
    @grep "DB_URL=" .env.supabase.tmp | sed 's/DB_URL=/SUPABASE_DB_URL=/' | sed 's/127.0.0.1/host.docker.internal/' | sed 's/postgres"/postgres?sslmode=disable"/' >> .env.supabase.tmp
    @echo "# Kafka Environment Variables" >> .env.supabase.tmp
    @echo "KAFKA_BROKERS=localhost:9093,localhost:9095,localhost:9097" >> .env.supabase.tmp
    @echo "KAFKA_DEFAULT_PARTITIONS=6" >> .env.supabase.tmp
    @echo "KAFKA_REPLICATION_FACTOR=3" >> .env.supabase.tmp
    @echo "KAFKA_MIN_INSYNC_REPLICAS=2" >> .env.supabase.tmp
    @echo "KAFKA_WAIT_TIMEOUT=60" >> .env.supabase.tmp
    @if [ -f .env ]; then (cat .env; echo ""; cat .env.supabase.tmp) > .env.supabase; else cp .env.supabase.tmp .env.supabase; fi
    @rm .env.supabase.tmp
    @echo "✅ Environment injection complete"

# === Development Workflow ===

# Quick development setup (first time)
setup: check setup-env supabase-init
    @echo "🎉 Setup complete! Run 'just dev' to start development environment"

# Start backend services only (for frontend hot-reload development)
dev-backend:
    @echo "🐳 Starting backend services for local frontend development..."
    @just supabase-start
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase up -d zookeeper kafka api worker dokito-backend
    @echo "✅ Backend services started!"
    @echo "🌐 Backend URLs:"
    @echo "   API:            http://localhost:8080"
    @echo "   Dokito Backend: http://localhost:8123"
    @echo "   Supabase:       http://localhost:54323"
    @echo ""
    @echo "🚀 Now run: cd frontend-next && npm run dev"

# Start frontend development server (run after dev-backend)
dev-frontend:
    @echo "🚀 Starting frontend development server..."
    cd frontend-next && npm run dev

# === Build Commands ===

# Rebuild just the API
api-rebuild:
    @echo "🏗️  Rebuilding API..."
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase build --no-cache api
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase up -d api
    @echo "✅ API rebuilt and restarted"

# Rebuild just the frontend
frontend-rebuild:
    @echo "🏗️  Rebuilding frontend..."
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase build --no-cache frontend
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase up -d frontend
    @echo "✅ Frontend rebuilt and restarted"

# Rebuild worker services
worker-rebuild:
    @echo "🏗️  Rebuilding workers..."
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase build --no-cache worker
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --env-file .env.supabase up -d worker
    @echo "✅ Workers rebuilt and restarted"

# === Debug Commands ===

# Start with debug services enabled (Kafka UI)
debug: supabase-start
    @echo "🐳 Starting with debug profile..."
    @just _inject-supabase-env docker-compose.dev.yml
    docker-compose -f docker-compose.yml -f docker-compose.dev.yml --profile debug --env-file .env.supabase up -d
    @echo "✅ Debug environment started!"
    @just show-services

# === Kafka Cluster Management ===

# Initialize Kafka cluster and topics
kafka-setup:
    @echo "🚀 Setting up Kafka cluster..."
    @./scripts/kafka-setup.sh setup

# Check Kafka cluster health
kafka-health:
    @echo "🔍 Checking Kafka cluster health..."
    @./scripts/kafka-setup.sh health

# Scale workers to match Kafka partitions
kafka-scale:
    @echo "⚡ Scaling workers to match Kafka partitions..."
    @./scripts/kafka-setup.sh scale

# Reset Kafka topics (DANGEROUS!)
kafka-reset:
    @echo "⚠️  Resetting Kafka topics..."
    @./scripts/kafka-setup.sh reset

# Wait for Kafka cluster to be ready
kafka-wait:
    @echo "⏳ Waiting for Kafka cluster..."
    @./scripts/kafka-setup.sh wait

# Create Kafka topics
kafka-topics:
    @echo "📝 Creating Kafka topics..."
    @./scripts/kafka-setup.sh topics

# Debug Kafka connectivity issues
kafka-debug:
    @echo "🔍 Debugging Kafka connectivity..."
    @echo "📋 Checking Kafka containers:"
    @docker ps --filter "name=kafka" --format "table {{{{.Names}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}"
    @echo ""
    @echo "📋 Checking Zookeeper containers:"
    @docker ps --filter "name=zookeeper" --format "table {{{{.Names}}}}\t{{{{.Status}}}}\t{{{{.Ports}}}}"
    @echo ""
    @echo "🔌 Testing external port connectivity:"
    @for port in 9093 9095 9097; do echo -n "Port $port: "; timeout 3 bash -c "</dev/tcp/localhost/$port" && echo "✅ Open" || echo "❌ Closed"; done
    @echo ""
    @echo "🩺 Running health check script:"
    @./scripts/kafka-setup.sh health || echo "Health check failed"

# === Production Deployment ===

# Link to production Supabase project
prod-link project_ref:
    @echo "🔗 Linking to production project: {{project_ref}}"
    supabase link --project-ref {{project_ref}}
    @echo "✅ Linked to production"

# Deploy database changes to production
prod-deploy-db:
    @echo "🚀 Deploying database changes to production..."
    supabase db push
    @echo "✅ Database deployed to production"

# Deploy functions to production
prod-deploy-functions:
    @echo "🚀 Deploying functions to production..."
    supabase functions deploy
    @echo "✅ Functions deployed to production"
