{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  nativeBuildInputs = [
    pkgs.rustup
    pkgs.gcc
    pkgs.llvmPackages.bintools-unwrapped
    pkgs.cmakeMinimal
  ];
}
