project(dragonstash)
cmake_minimum_required(VERSION 3.12)

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

## SUBMODULE: Catch2

add_subdirectory(Catch2)

## SUBMODULE: CLI11

add_subdirectory(CLI11)

# MAIN THING

set(DRAGONSTASH_HEADERS
    include/dragonstash/dragonstash-config.h
    include/dragonstash/backend/base.hpp
    include/dragonstash/backend/in_memory.hpp
    include/dragonstash/backend/local.hpp
    include/dragonstash/cache/blocklist.hpp
    include/dragonstash/cache/cache.hpp
    include/dragonstash/cache/common.hpp
    include/dragonstash/cache/direntry.hpp
    include/dragonstash/cache/inode.hpp
    include/dragonstash/debug_mutex.hpp
    include/dragonstash/error.hpp
    include/dragonstash/fuse/buffer.hpp
    include/dragonstash/fuse/interface.hpp
    include/dragonstash/fuse/request.hpp
    include/dragonstash/fs.hpp
    )

set(DRAGONSTASH_SRCS
    src/backend/base.cpp
    src/backend/in_memory.cpp
    src/backend/local.cpp
    src/cache/blocklist.cpp
    src/cache/cache.cpp
    src/cache/direntry.cpp
    src/cache/inode.cpp
    src/debug_mutex.cpp
    src/error.cpp
    src/fuse/buffer.cpp
    src/fuse/interface.cpp
    src/fuse/request.cpp
    src/fs.cpp)

set(DRAGONSTASH_FLAGS -Wall -Wno-missing-field-initializers -Wno-comment -Wno-unused-parameter -Werror -Wextra)

add_library(dragonstash STATIC ${DRAGONSTASH_SRCS} ${DRAGONSTASH_HEADERS})
target_link_libraries(dragonstash PkgConfig::FUSE3)
target_link_libraries(dragonstash lmdb-safe)
target_include_directories(dragonstash PUBLIC include)
target_compile_options(dragonstash PRIVATE ${DRAGONSTASH_FLAGS})

add_executable(dragonstashfs src/main.cpp)
target_link_libraries(dragonstashfs dragonstash)
target_link_libraries(dragonstashfs CLI11)
target_compile_options(dragonstashfs PRIVATE ${DRAGONSTASH_FLAGS})

# UNIT TESTS

set(TESTS_SRCS
    tests/main.cpp
    tests/backend/in_memory.cpp
    tests/fs.cpp
    tests/cache/cache.cpp
    tests/cache/inode.cpp
    tests/cache/blocklist.cpp
    tests/testutils/tempdir.cpp
    tests/testutils/fuse_backend.cpp)

add_executable(dragonstash-tests ${TESTS_SRCS})
target_link_libraries(dragonstash-tests Catch2::Catch2 lmdb-safe dragonstash)
target_include_directories(dragonstash-tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")


# PLAYGROUND

set(PLAYGROUND_SRCS
    playground/main.cpp)
add_executable(playground ${PLAYGROUND_SRCS})
target_link_libraries(playground lmdb-safe)
