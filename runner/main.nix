{ pkgs, system, gomod2nix }:

let
  # Build the CLI binary
  goCLI = gomod2nix.legacyPackages.${system}.buildGoApplication {
    pname = "dokito-cli";
    version = "0.1.0";
    src = ./.;
    modules = ./gomod2nix.toml;
    subPackages = [ "./cmd/dokito-cli" ];

    meta = {
      description = "Dokito job processing CLI debug tool";
      platforms = pkgs.lib.platforms.linux ++ pkgs.lib.platforms.darwin;
    };
  };

  # Build the server binary
  goServer = gomod2nix.legacyPackages.${system}.buildGoApplication {
    pname = "dokito-server";
    version = "0.1.0";
    src = ./.;
    modules = ./gomod2nix.toml;
    subPackages = [ "./cmd/dokito-server" ];

    meta = {
      description = "Dokito job processing API server";
      platforms = pkgs.lib.platforms.linux ++ pkgs.lib.platforms.darwin;
    };
  };

in {
  packages = {
    dokito-cli-binary = goCLI;
    dokito-server-binary = goServer;
  };

  # Export the binaries for use in wrapper scripts
  binaries = {
    cli = "${goCLI}/bin/dokito-cli";
    server = "${goServer}/bin/dokito-server";
  };
}