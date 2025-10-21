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
            export BINARY_EXECUTION_PATH=$(pwd)
          '';

          # Debug function that reads and displays the actual environment variables
          dokitoEnvDebug = ''
            echo "🔧 Environment configured:"
            echo "  NYPUC: $OPENSCRAPER_PATH_NYPUC"
            echo "  COPUC: $OPENSCRAPER_PATH_COPUC"
            echo "  UtahCoal: $OPENSCRAPER_PATH_UTAHCOAL"
            echo "  Process: $DOKITO_PROCESS_DOCKETS_BINARY_PATH"
            echo "  Upload: $DOKITO_UPLOAD_DOCKETS_BINARY_PATH"
            echo "  Download: $DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH"
            echo "  Current Directory: $BINARY_EXECUTION_PATH"
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

        in {
          # Packages from both modules plus server
          packages = {
            default = dokitoComplete;
            dokito-complete = dokitoComplete;
            dokito-cli = dokitoCLI;
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
          } // playwrightModule.apps // rustModule.apps;
        };
    };
}
