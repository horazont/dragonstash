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

# MAIN THING

set(DRAGONSTASH_HEADERS
    include/dragonstash-config.h
    include/backend.hpp
    include/cache/blocklist.hpp
    include/cache/cache.hpp
    include/cache/common.hpp
    include/cache/inode.hpp
    include/debug_mutex.hpp
    include/error.hpp
    include/fuse/buffer.hpp
    include/fuse/interface.hpp
    include/fuse/request.hpp
    include/fs.hpp
    include/in_memory_backend.hpp
    include/local_backend.hpp
    )

set(DRAGONSTASH_SRCS
    src/backend.cpp
    src/cache/blocklist.cpp
    src/cache/cache.cpp
    src/cache/inode.cpp
    src/debug_mutex.cpp
    src/error.cpp
    src/fuse/buffer.cpp
    src/fuse/interface.cpp
    src/fuse/request.cpp
    src/in_memory_backend.cpp
    src/local_backend.cpp
    src/fs.cpp)

add_library(libdragonstash STATIC ${DRAGONSTASH_SRCS} ${DRAGONSTASH_HEADERS})
target_link_libraries(libdragonstash PkgConfig::FUSE3)
target_link_libraries(libdragonstash lmdb-safe)
target_include_directories(libdragonstash PUBLIC include)
target_compile_options(libdragonstash PRIVATE -Wall -Wno-missing-field-initializers -Wno-comment -Wno-unused-parameter -Werror -Wextra)

add_executable(dragonstash src/main.cpp)
target_link_libraries(dragonstash libdragonstash)
target_compile_options(dragonstash PRIVATE -Wall -Wno-missing-field-initializers -Wno-comment -Wno-unused-parameter -Werror -Wextra)

# UNIT TESTS

set(TESTS_SRCS
    tests/main.cpp
    tests/in_memory_backend.cpp
    tests/fs.cpp
    tests/cache/cache.cpp
    tests/cache/inode.cpp
    tests/cache/blocklist.cpp
    tests/testutils/tempdir.cpp
    tests/testutils/fuse_backend.cpp)

add_executable(dragonstash-tests ${TESTS_SRCS})
target_link_libraries(dragonstash-tests Catch2::Catch2 lmdb-safe libdragonstash)
target_include_directories(dragonstash-tests PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")


# PLAYGROUND

set(PLAYGROUND_SRCS
    playground/main.cpp)
add_executable(playground ${PLAYGROUND_SRCS})
target_link_libraries(playground lmdb-safe)
