{ pkgs ? import <nixpkgs> {} }:
with pkgs;
let
  kafka-mirrormaker = rustPlatform.buildRustPackage rec {
        pname = "kafka-mm";
        version = "0.1.0";

        src = ./kafka-mm;

        cargoSha256 = "1k5bivnm8gjd44f6ry07wgcqc8y10g09dcgr9j03fv8mq62l2562";

        nativeBuildInputs = [
          cmakeMinimal
        ];
      };
in
  dockerTools.buildImage {
    name = "kafka-mirrormaker";
    tag = "0.1.0";
    created = "now";
    config = {
      Entrypoint = [ "${kafka-mirrormaker}/bin/kafka-mirrormaker" ];
    };
  }
