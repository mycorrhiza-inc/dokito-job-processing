{ pkgs, pkgs-playwright, system }:

let
  # Import the node2nix generated packages
  nodePackages = import ./default.nix {
    inherit pkgs system;
    nodejs = pkgs.nodejs_20;  # Use Node.js 20 instead of default 14
  };

  # Create a wrapper script for each scraper
  mkScraperApp = { name, script }: {
    type = "app";
    program = "${pkgs.writeShellScript "scraper-${name}" ''
      set -euo pipefail

      # Common environment setup
      export PLAYWRIGHT_BROWSERS_PATH="${pkgs-playwright.playwright-driver.browsers}"
      export PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
      export PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS=true
      export PLAYWRIGHT_NODEJS_PATH="${pkgs.nodejs_20}/bin/node"
      export NODE_PATH="${nodePackages.package}/lib/node_modules/js_scrapers/node_modules"

      cd "${nodePackages.package}/lib/node_modules/js_scrapers"
      echo "Running ${name} scraper..." >&2
      ${pkgs.nodePackages.ts-node}/bin/ts-node ${script} "$@"
    ''}";
  };

  # Define all available scrapers
  scrapers = {
    ny-puc = "ny_puc_scraper.spec.ts";
    co-puc = "co_puc_scraper.copied-spec.ts";
    utah-coal = "utah_coal_grand_scraper.spec.ts";
  };

in {
  # Package derivation
  packages = {
    playwright-scrapers = nodePackages.package;
  };

  # Apps for easy running - busybox style with multiple scrapers
  apps = builtins.mapAttrs (name: script: mkScraperApp { inherit name script; }) scrapers;
}