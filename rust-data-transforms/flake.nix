{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, naersk, nix2container }:
    let
      # Support multiple host systems
      hostSystems = [ "x86_64-linux" "aarch64-darwin" "x86_64-darwin" ];
      # Always target x86_64-linux for containers
      targetSystem = "x86_64-linux";

      # Generate packages for each host system
      forAllSystems = nixpkgs.lib.genAttrs hostSystems;

      # Get packages for cross-compilation
      getPkgs = hostSystem:
        if hostSystem == targetSystem then
          # Native compilation
          nixpkgs.legacyPackages.${hostSystem}
        else
          # Cross-compilation
          import nixpkgs {
            system = hostSystem;
            crossSystem = { system = targetSystem; };
          };

      # Build function that works for any system
      mkDokitoBackend = hostSystem:
        let
          pkgs = getPkgs hostSystem;
          naersk' = pkgs.callPackage naersk {};
        in
        naersk'.buildPackage {
          src = ./.;
          name = "dokito_processing_monolith";

          # Build only the main binary, not the whole workspace
          cargoBuildOptions = x: x ++ [ "--package" "dokito_processing_monolith" ];

          # Add any additional build inputs if needed
          nativeBuildInputs = with pkgs; [
            pkg-config
          ];

          buildInputs = with pkgs; [
            openssl
          ];

          # Set environment variables for OpenSSL
          OPENSSL_NO_VENDOR = 1;
          PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
        };


    in {
      packages = forAllSystems (system:
        let
          dokito-backend = mkDokitoBackend system;
          container = mkContainer system;
        in {
          default = dokito-backend;
          dokito-backend = dokito-backend;
          container = container;
        });


      # Apps for easy running
      apps = forAllSystems (system:
        let
          pkgs = getPkgs system;
          dokito-backend = mkDokitoBackend system;
        in {
          default = {
            type = "app";
            program = "${dokito-backend}/bin/dokito_processing_monolith";
          };

        });
    };
}
