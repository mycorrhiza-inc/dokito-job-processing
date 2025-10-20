{
  description = "Dokito job processing monorepo with Playwright scrapers and Rust data transforms";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    playwright-pkgs = {
      url = "github:pietdevries94/nixpkgs-playwright";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs@{ flake-parts, nixpkgs, naersk, playwright-pkgs, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      perSystem = { config, self', inputs', pkgs, system, ... }:
        let
          # Import component modules
          playwrightModule = import ./playwright/nix.nix {
            inherit pkgs system;
            pkgs-playwright = playwright-pkgs.legacyPackages.${system};
          };

          rustModule = import ./rust-data-transforms/nix.nix {
            inherit pkgs system naersk;
          };

          # Build Go server
          goServer = pkgs.buildGoModule {
            pname = "dokito-job-processing-server";
            version = "0.1.0";
            src = ./runner;

            vendorHash = null; # No external dependencies

            buildInputs = with pkgs; [ ];
            nativeBuildInputs = with pkgs; [ go ];

            meta = {
              description = "Dokito job processing API server";
              platforms = pkgs.lib.platforms.linux ++ pkgs.lib.platforms.darwin;
            };
          };

          # Create a wrapper that sets up environment variables
          dokitoComplete = pkgs.writeShellScriptBin "dokito-complete" ''
            set -euo pipefail

            # Set scraper binary paths (these point to the app programs)
            export OPENSCRAPER_PATH_NYPUC="${playwrightModule.apps.ny-puc.program}"
            export OPENSCRAPER_PATH_COPUC="${playwrightModule.apps.co-puc.program}"
            export OPENSCRAPER_PATH_UTAHCOAL="${playwrightModule.apps.utah-coal.program}"

            # Set dokito binary paths
            export DOKITO_PROCESS_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/dokito_processing_monolith"
            export DOKITO_UPLOAD_DOCKETS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/dokito_processing_monolith"
            export DOKITO_DOWNLOAD_ATTACHMENTS_BINARY_PATH="${rustModule.packages.dokito-backend}/bin/dokito_processing_monolith"

            echo "🔧 Environment configured:"
            echo "  NYPUC: ${playwrightModule.apps.ny-puc.program}"
            echo "  COPUC: ${playwrightModule.apps.co-puc.program}"
            echo "  UtahCoal: ${playwrightModule.apps.utah-coal.program}"
            echo "  Dokito: ${rustModule.packages.dokito-backend}/bin/dokito_processing_monolith"
            echo ""

            # Execute the server
            exec "${goServer}/bin/dokito-job-processing-server" "$@"
          '';

        in {
          # Packages from both modules plus server
          packages = {
            default = dokitoComplete;
            dokito-complete = dokitoComplete;
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
          } // playwrightModule.apps // rustModule.apps;
        };
    };
}