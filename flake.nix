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

          # Build Go server using gomod2nix
          goServer = gomod2nix.legacyPackages.${system}.buildGoApplication {
            pname = "dokito-job-processing-server";
            version = "0.1.0";
            src = ./runner;
            modules = ./runner/gomod2nix.toml;

            meta = {
              description = "Dokito job processing API server";
              platforms = pkgs.lib.platforms.linux ++ pkgs.lib.platforms.darwin;
            };
          };

          # Create a wrapper that sets up environment variables for the server
          dokitoComplete = pkgs.writeShellScriptBin "dokito-complete" ''
            set -euo pipefail

            # Set scraper binary paths (these point to the app programs)
            export OPENSCRAPER_PATH_NYPUC="${playwrightModule.apps.ny-puc.program}"
            export OPENSCRAPER_PATH_COPUC="${playwrightModule.apps.co-puc.program}"
            export OPENSCRAPER_PATH_UTAHCOAL="${playwrightModule.apps.utah-coal.program}"

            # Set dokito binary paths
            export DOKITO_PROCESS_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/process-dockets"
            export DOKITO_UPLOAD_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/upload-dockets"
            export DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/download-attachments"

            echo "🔧 Environment configured:"
            echo "  NYPUC: ${playwrightModule.apps.ny-puc.program}"
            echo "  COPUC: ${playwrightModule.apps.co-puc.program}"
            echo "  UtahCoal: ${playwrightModule.apps.utah-coal.program}"
            echo "  Process: ${rustModule.packages.dokito-backend}/bin/process-dockets"
            echo "  Upload: ${rustModule.packages.dokito-backend}/bin/upload-dockets"
            echo "  Download: ${rustModule.packages.dokito-backend}/bin/download-attachments"
            echo "  Current Directory: $(pwd)"
            echo $(pwd)
            echo ""

            # Execute the server
            exec "${goServer}/bin/runner" "$@"
          '';

          # Create a CLI wrapper that sets up environment variables and runs in CLI mode
          dokitoCLI = pkgs.writeShellScriptBin "dokito-cli" ''
            set -euo pipefail

            # Set scraper binary paths (these point to the app programs)
            export OPENSCRAPER_PATH_NYPUC="${playwrightModule.apps.ny-puc.program}"
            export OPENSCRAPER_PATH_COPUC="${playwrightModule.apps.co-puc.program}"
            export OPENSCRAPER_PATH_UTAHCOAL="${playwrightModule.apps.utah-coal.program}"

            # Set dokito binary paths
            export DOKITO_PROCESS_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/process-dockets"
            export DOKITO_UPLOAD_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/upload-dockets"
            export DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/download-attachments"

            # Execute the CLI
            exec "${goServer}/bin/runner" "$@"
          '';

        in {
          # Packages from both modules plus server
          packages = {
            default = dokitoComplete;
            dokito-complete = dokitoComplete;
            dokito-cli = dokitoCLI;
            go-server = goServer;
          } // playwrightModule.packages // rustModule.packages;

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
