{
  description = "Dokito job processing monorepo with Playwright scrapers and Rust data transforms";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    gomod2nix = {
      url = "github:nix-community/gomod2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ flake-parts, nixpkgs, naersk, gomod2nix, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          # Import component modules
          playwrightModule = import ./playwright/main.nix {
            inherit pkgs system;
            pkgs-playwright = pkgs; # Use standard nixpkgs for playwright
          };

          rustModule = import ./rust-data-transforms/main.nix {
            inherit pkgs system naersk;
          };

          runnerModule = import ./runner/main.nix {
            inherit pkgs system gomod2nix;
          };

          # Environment variable definitions
          secretEnvVars = [
            "DATABASE_URL"
            "DIGITALOCEAN_S3_ACCESS_KEY"
            "DIGITALOCEAN_S3_SECRET_KEY"
            "DEEPINFRA_API_KEY"
          ];

          publicEnvVars = {
            DIGITALOCEAN_S3_ENDPOINT = "https://sfo3.digitaloceanspaces.com";
            DIGITALOCEAN_S3_REGION = "sfo3";
            DOKITO_S3_HTML_BUCKET = "flaoq";
            OPENSCRAPERS_S3_OBJECT_BUCKET = "openscrapers";
            DOKITO_INGEST_REDIS = "http://localhost:6379";
          };

          # Environment validation function
          envValidation = ''
            echo "🔍 Validating environment configuration..."

            # Check secret environment variables
            missing_secrets=()
            ${builtins.concatStringsSep "\n" (map (var: ''
              if [ -z "''${${var}:-}" ]; then
                missing_secrets+=("${var}")
              fi
            '') secretEnvVars)}

            if [ ''${#missing_secrets[@]} -gt 0 ]; then
              echo "❌ Missing required secret environment variables:"
              for var in "''${missing_secrets[@]}"; do
                echo "   - $var"
              done
              echo ""
              echo "💡 Please copy example.env to .env and set the required values:"
              echo "   cp example.env .env"
              echo "   # Edit .env with your actual values"
              echo "   source .env"
              echo ""
              exit 1
            fi

            # Set defaults for public variables and log when using defaults
            echo "📝 Public environment variables:"
            ${builtins.concatStringsSep "\n" (map (var: let
              defaultValue = publicEnvVars.${var};
            in ''
              if [ -z "''${${var}:-}" ]; then
                export ${var}="${defaultValue}"
                echo "   ${var}: Using default (${defaultValue})"
              else
                echo "   ${var}: Using custom value (''${${var}})"
              fi
            '') (builtins.attrNames publicEnvVars))}

            echo "✅ All secret environment variables are set"
            echo ""
          '';

          # Common environment variable setup function
          dokitoEnvSetup = ''
            # Set scraper binary paths (these point to the app programs)
            export OPENSCRAPER_PATH_NYPUC="${playwrightModule.apps.ny-puc.program}"
            export OPENSCRAPER_PATH_COPUC="${playwrightModule.apps.co-puc.program}"
            export OPENSCRAPER_PATH_UTAHCOAL="${playwrightModule.apps.utah-coal.program}"

            # Set dokito binary paths
            export DOKITO_PROCESS_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/process-dockets"
            export DOKITO_UPLOAD_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/upload-dockets"
            export DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/download-attachments"
            export DOKITO_DATABASE_UTILS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/database-utils"
            export BINARY_EXECUTION_PATH=$(pwd)

            # Set up Redis configuration for author caching
            export REDIS_DATA_DIR="$BINARY_EXECUTION_PATH/redis_data"
            export REDIS_URL="redis://127.0.0.1:6379"
          '';

          # Debug function that reads and displays the actual environment variables
          dokitoEnvDebug = ''
            ${envValidation}
            echo "🔧 Environment configured:"
            echo "  NYPUC: $OPENSCRAPER_PATH_NYPUC"
            echo "  COPUC: $OPENSCRAPER_PATH_COPUC"
            echo "  UtahCoal: $OPENSCRAPER_PATH_UTAHCOAL"
            echo "  Process: $DOKITO_PROCESS_DOCKETS_BINARY_PATH"
            echo "  Upload: $DOKITO_UPLOAD_DOCKETS_BINARY_PATH"
            echo "  Download: $DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"
            echo "  Database: $DOKITO_DATABASE_UTILS_BINARY_PATH"
            echo "  Current Directory: $BINARY_EXECUTION_PATH"
            echo "  Redis URL: $REDIS_URL"
            echo "  Redis Data: $REDIS_DATA_DIR"
            echo ""

            # Check Redis connectivity
            echo "🔍 Checking Redis connectivity..."
            if ${pkgs.redis}/bin/redis-cli ping >/dev/null 2>&1; then
              echo "✅ Redis server responsive"
              echo "   Cache keys: $(${pkgs.redis}/bin/redis-cli dbsize 2>/dev/null || echo 'unknown')"
            else
              echo "❌ Redis server not responding"
              echo "   Author caching will be disabled"
              echo "   To enable caching, run: nix run .#redis"
            fi
            echo ""

            # Check database connectivity
            echo "🔍 Checking database connectivity..."
            echo "  Database URL: Set (checking connection...)"
            if ! ${pkgs.postgresql}/bin/psql "''${DATABASE_URL}" -c "SELECT 1;" >/dev/null 2>&1; then
              echo "❌ Database connection failed!"
              echo "   URL pattern: $(echo "''${DATABASE_URL}" | sed 's/:\/\/.*@/:\/\/<REDACTED>@/')"
              echo "   This will cause processing and upload steps to fail."
              echo "   Please check your database credentials and connectivity."
              exit 1
            else
              echo "✅ Database connection successful"
            fi
            echo ""
          '';

          # Create a wrapper that sets up environment variables for the server
          dokitoComplete = pkgs.writeShellScriptBin "dokito-complete" ''
            set -euo pipefail

            ${dokitoEnvSetup}
            ${dokitoEnvDebug}

            # Execute the server
            exec "${runnerModule.binaries.server}" "$@"
          '';

          # Create a CLI wrapper that sets up environment variables and runs in CLI mode
          dokitoCLI = pkgs.writeShellScriptBin "dokito-cli" ''
            set -euo pipefail

            ${dokitoEnvSetup}
            ${dokitoEnvDebug}

            # Execute the CLI
            exec "${runnerModule.binaries.cli}" "$@"
          '';

          # Create a Redis server wrapper for author caching
          dokitoRedis = pkgs.writeShellScriptBin "dokito-redis" ''
            set -euo pipefail

            export BINARY_EXECUTION_PATH=$(pwd)
            export REDIS_DATA_DIR="$BINARY_EXECUTION_PATH/redis_data"

            # Create Redis data directory if it doesn't exist
            mkdir -p "$REDIS_DATA_DIR"

            echo "🚀 Starting Redis server for dokito author caching..."
            echo "   Data directory: $REDIS_DATA_DIR"
            echo "   URL: redis://127.0.0.1:6379"
            echo ""

            # Start Redis server in foreground mode
            exec ${pkgs.redis}/bin/redis-server \
              --port 6379 \
              --dir "$REDIS_DATA_DIR" \
              --dbfilename "dokito-cache.rdb" \
              --save 60 1000 \
              --maxmemory 256mb \
              --maxmemory-policy allkeys-lru \
              --logfile "$REDIS_DATA_DIR/redis.log" \
              --loglevel notice
          '';

        in {
          # Packages from both modules plus server
          packages = {
            default = dokitoComplete;
            dokito-complete = dokitoComplete;
            dokito-cli = dokitoCLI;
            dokito-redis = dokitoRedis;
          } // playwrightModule.packages // rustModule.packages // runnerModule.packages;

          # Apps
          apps = {
            default = {
              type = "app";
              program = "${dokitoComplete}/bin/dokito-complete";
            };
            server = {
              type = "app";
              program = "${dokitoComplete}/bin/dokito-complete";
            };
            cli = {
              type = "app";
              program = "${dokitoCLI}/bin/dokito-cli";
            };
            redis = {
              type = "app";
              program = "${dokitoRedis}/bin/dokito-redis";
            };
          } // playwrightModule.apps // rustModule.apps;
        };
    };
}
