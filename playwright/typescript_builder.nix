{ pkgs, system ? builtins.currentSystem, nodejs ? pkgs.nodejs_24 }:

let
  # Create a derivation that handles npm dependencies
  nodeModules = pkgs.buildNpmPackage {
    pname = "js_scrapers-deps";
    version = "1.0.0";
    src = ./.;

    # Use Node.js 24
    inherit nodejs;

    # Hash for the node_modules (with TypeScript as dependency)
    npmDepsHash = "sha256-Kn35iQocpRK4/bhc/TVCkGdGkpn+kr2XoNPNhBHSobU=";

    # Install dev dependencies for TypeScript
    npmInstallFlags = [ "--include=dev" ];

    # Don't run build, just install dependencies
    dontNpmBuild = true;
  };

  # Create the main package with TypeScript compilation
  package = pkgs.stdenv.mkDerivation {
    pname = "js_scrapers";
    version = "1.0.0";
    src = ./.;

    buildInputs = [ nodejs ];

    buildPhase = ''
      # Link in node_modules (buildNpmPackage uses the package name from package.json)
      ln -sf ${nodeModules}/lib/node_modules/js_scrapers/node_modules ./node_modules

      # TypeScript compilation with type checking
      echo "🔨 Compiling TypeScript with Node.js $(node --version)"
      echo "📁 TypeScript compiler: $(${nodejs}/bin/node ./node_modules/typescript/bin/tsc --version)"

      # Run TypeScript compiler (this will fail the build if there are type errors)
      # Note: Using --noEmit for type checking, without --strict for now to allow existing code
      ${nodejs}/bin/node ./node_modules/typescript/bin/tsc --noEmit
      echo "✅ Type checking passed!"

      # Compile to JavaScript
      ${nodejs}/bin/node ./node_modules/typescript/bin/tsc
      echo "✅ TypeScript compilation completed!"
    '';

    installPhase = ''
      mkdir -p $out/{bin,lib/js_scrapers}

      # Copy compiled JavaScript and other files
      cp -r dist/ $out/lib/js_scrapers/ 2>/dev/null || true
      cp -r *.ts *.json $out/lib/js_scrapers/ 2>/dev/null || true

      # Link node_modules
      ln -sf ${nodeModules}/lib/node_modules/js_scrapers/node_modules $out/lib/js_scrapers/node_modules
    '';
  };

in {
  inherit package nodeModules;
}