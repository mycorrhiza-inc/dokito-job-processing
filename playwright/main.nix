{ pkgs, pkgs-playwright, system }:

let
  # Import the pure Nix packages
  nodePackages = import ./typescript_builder.nix {
    inherit pkgs system;
    nodejs = pkgs.nodejs_24;
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
      export PLAYWRIGHT_NODEJS_PATH="${pkgs.nodejs_24}/bin/node"
      export NODE_PATH="${nodePackages.package}/lib/js_scrapers/node_modules"

      cd "${nodePackages.package}/lib/js_scrapers"
      echo "Running ${name} scraper..." >&2
      ${pkgs.nodejs_24}/bin/node dist/${builtins.replaceStrings [".ts"] [".js"] script} "$@"
    ''}";
  };

  # Define all available scrapers
  scrapers = {
    ny-puc = "ny_puc_scraper.spec.ts";
    co-puc = "co_puc_scraper.spec.ts";
    utah-coal = "utah_coal_grand_scraper.spec.ts";
  };

in {
  # Package derivation
  packages = {
    playwright-scrapers = nodePackages.package;
    nodeModules = nodePackages.nodeModules;
  };

  # Apps for easy running - busybox style with multiple scrapers
  apps = builtins.mapAttrs (name: script: mkScraperApp { inherit name script; }) scrapers;
}
