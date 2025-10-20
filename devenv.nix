{ pkgs, lib, config, inputs, ... }:
{
  # Base configuration shared across all project devenvs
  dotenv.enable = true;
  dotenv.disableHint = true;
  cachix.enable = false;

  # Common packages available to all projects
  languages.rust.enable = true;
  languages.go.enable = true;

  packages = with pkgs; [
    openssl
    node2nix
    inputs.gomod2nix.packages.${pkgs.system}.default
    go-swag
  ];

  env = {
    
  };


  # See full reference at https://devenv.sh/reference/options/
}
