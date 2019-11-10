#!/bin/bash
set -euo pipefail

sudo python3 -m pip install pytest meson
mkdir dependencies
git clone --depth 1 --single-branch -b fuse-3.7.0 https://github.com/libfuse/libfuse dependencies/libfuse
pushd dependencies/libfuse
mkdir build
cd build
meson ..
ninja
sudo ninja install
popd

pkg-config --libs fuse3
pkg-config --cflags fuse3
