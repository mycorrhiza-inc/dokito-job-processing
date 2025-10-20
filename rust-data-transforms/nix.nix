{ pkgs, system, naersk }:

let
  naersk' = pkgs.callPackage naersk {};

  dokito-backend = naersk'.buildPackage {
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
  packages = {
    default = dokito-backend;
    dokito-backend = dokito-backend;
  };

  apps = {
    default = {
      type = "app";
      program = "${dokito-backend}/bin/dokito_processing_monolith";
    };
  };
}