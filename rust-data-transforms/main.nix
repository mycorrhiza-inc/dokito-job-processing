{ pkgs, system, naersk }:

let
  naersk' = pkgs.callPackage naersk {};

  dokito-backend = naersk'.buildPackage {
    src = ./.;
    name = "rust-data-transforms";

    # Build the rust-data-transforms package
    cargoBuildOptions = x: x ++ [ "--package" "rust-data-transforms" ];

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
    dokito-backend = dokito-backend;
  };

  apps = {
    process-dockets = {
      type = "app";
      program = "${dokito-backend}/bin/process-dockets";
    };
    upload-dockets = {
      type = "app";
      program = "${dokito-backend}/bin/upload-dockets";
    };
    download-attachments = {
      type = "app";
      program = "${dokito-backend}/bin/download-attachments";
    };
    database-utils = {
      type = "app";
      program = "${dokito-backend}/bin/database-utils";
    };
  };
}
