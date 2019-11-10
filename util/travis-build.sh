#!/bin/bash
set -xeuo pipefail
mkdir build
cd build
CC="$(which gcc-9)"
CXX="$(which g++-9)"
cmake ..
make
