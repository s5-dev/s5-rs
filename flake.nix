{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
            ];
          };
        in
        {
          default = pkgs.mkShell {
            buildInputs = [
              rustToolchain
              pkgs.cargo-deny
              pkgs.cargo-nextest
              pkgs.sccache
              pkgs.pkg-config
              pkgs.clang
              pkgs.mold
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.isLinux [ pkgs.fuse3 ]
            ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
              pkgs.darwin.apple_sdk.frameworks.Security
              pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
            ];

            env = {
              # RUSTFLAGS in mkShell shadows the parent shell's value, so any
              # RUSTFLAGS set at the GitHub Actions workflow level would be
              # overwritten the moment we enter `nix develop`. Keep all rustc
              # flags here so dev and CI see exactly the same set.
              RUSTFLAGS = "-C linker=clang -C link-arg=-fuse-ld=mold -D warnings";
              RUSTC_WRAPPER = "sccache";
            };
          };
        }
      );
    };
}
