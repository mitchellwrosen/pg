{
  description = "pg";

  inputs = {
    nixpkgs = {
      url = "github:NixOS/nixpkgs/22.11";
    };
  };

  outputs = { self, nixpkgs }:
    {
      devShell = {
        x86_64-linux =
          nixpkgs.legacyPackages.x86_64-linux.mkShell {
            buildInputs = [
              nixpkgs.legacyPackages.x86_64-linux.gmp
              nixpkgs.legacyPackages.x86_64-linux.openssl
              nixpkgs.legacyPackages.x86_64-linux.pkgconfig
              nixpkgs.legacyPackages.x86_64-linux.postgresql
            ];
          };
      };
    };
}
