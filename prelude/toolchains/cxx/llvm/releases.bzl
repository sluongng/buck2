# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# This source code is dual-licensed under either the MIT license found in the
# LICENSE-MIT file in the root directory of this source tree or the Apache
# License, Version 2.0 found in the LICENSE-APACHE file in the root directory
# of this source tree. You may select, at your option, one of the
# above-listed licenses.

# Metadata copied from hermeticbuild/hermetic-llvm llvm@0.8.6
# extensions/llvm_toolchain_minimal_index.json.
llvm_minimal_releases = {
    "latest_by_llvm_version": {
        "22.1.7": "llvm-22.1.7-1",
    },
    "releases": {
        "llvm-22.1.7-1": {
            "darwin-amd64": {
                "sha256": "ee5b1e8b7bc2914da7439850321d7216d6367dfb138c4faa6bc6a5c046abaf21",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-darwin-amd64.tar.zst",
            },
            "darwin-arm64": {
                "sha256": "83259015d1e7fe11cd3ab14df2f442da2754c88fb502f1e81789d2ed8ff166a2",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-darwin-arm64.tar.zst",
            },
            "linux-amd64-musl": {
                "sha256": "8005a453f3f870bfd19ceda7781ed85d41a9d976d8a40a747f88ca41665b4315",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-linux-amd64-musl.tar.zst",
            },
            "linux-arm64-musl": {
                "sha256": "ba5f8078fd665fd43c8c5d1fcffd4908e130e2b6a4fdf1e281316508f6e9e9fb",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-linux-arm64-musl.tar.zst",
            },
            "windows-amd64": {
                "sha256": "d8a302fb3d752aa7cd18800ec4cf52ca81c1c6cd547ecfa913629c7a7ea9202d",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-windows-amd64.tar.zst",
            },
            "windows-arm64": {
                "sha256": "6adee51f1cdf8bce4141b031e865ff012f359b36d527cbfb74064441328785be",
                "url": "https://github.com/hermeticbuild/hermetic-llvm/releases/download/llvm-22.1.7-1/llvm-toolchain-minimal-22.1.7-windows-arm64.tar.zst",
            },
        },
    },
}
