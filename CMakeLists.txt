project(dragonstash)
cmake_minimum_required(VERSION 3.13)

set (CMAKE_CXX_STANDARD 17)

# DEPENDENCIES

find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED QUIET IMPORTED_TARGET fuse3)
pkg_check_modules(LMDB REQUIRED QUIET IMPORTED_TARGET lmdb)

# SUBMODDULES

## SUBMODULE: lmdb-safe

set(LMDB_SAFE_DIR lmdb-safe)
set(LMDB_SAFE_SRCS ${LMDB_SAFE_DIR}/lmdb-safe.cc)

add_library(lmdb-safe STATIC ${LMDB_SAFE_SRCS})
target_link_libraries(lmdb-safe PkgConfig::LMDB)
target_include_directories(lmdb-safe PUBLIC ${LMDB_SAFE_DIR})

# MAIN THING

set(DRAGONSTASH_HEADERS
    include/dragonstash-config.h
    include/backend.hpp
    include/cache/cache.hpp
    include/error.hpp
    include/fuse/buffer.hpp
    include/fuse/interface.hpp
    include/fuse/request.hpp
    include/fs.hpp
    include/local_backend.hpp
    )

set(DRAGONSTASH_SRCS
    src/backend.cpp
    src/cache/cache.cpp
    src/error.cpp
    src/fuse/buffer.cpp
    src/fuse/interface.cpp
    src/fuse/request.cpp
    src/local_backend.cpp
    src/main.cpp
    src/fs.cpp)

add_executable(dragonstash ${DRAGONSTASH_SRCS} ${DRAGONSTASH_HEADERS})
target_link_libraries(dragonstash PkgConfig::FUSE3)
target_link_libraries(dragonstash lmdb-safe)
target_include_directories(dragonstash PRIVATE include)


# PLAYGROUND

set(PLAYGROUND_SRCS
    playground/main.cpp)
add_executable(playground ${PLAYGROUND_SRCS})
target_link_libraries(playground lmdb-safe)
