{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          nativeBuildInputs = [
            pkgs.cargo
            pkgs.rustc
            pkgs.gcc
            pkgs.pkg-config
            pkgs.gnumake
          ];

          buildInputs = [
            pkgs.openssl
            pkgs.glib
            pkgs.gst_all_1.gstreamer
            pkgs.gst_all_1.gst-plugins-base
            pkgs.gst_all_1.gst-rtsp-server
          ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          ];

          RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";

          NIX_CFLAGS_COMPILE = "-Wno-error=int-conversion";
        };
      });
    };
}
