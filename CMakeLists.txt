project(dragonstash)
cmake_minimum_required(VERSION 3.13)

# DEPENDENCIES

find_package(PkgConfig REQUIRED)
pkg_check_modules(FUSE3 REQUIRED QUIET IMPORTED_TARGET fuse3)

# SUBMODDULES

## SUBMODULE: Fusepp

set(FUSEPP_DIR Fusepp)
set(FUSEPP_SRCS ${FUSEPP_DIR}/Fuse.cpp ${FUSEPP_DIR}/Fuse-impl.h)

add_library(fusepp STATIC ${FUSEPP_SRCS})
target_link_libraries(fusepp PkgConfig::FUSE3)
target_include_directories(fusepp PUBLIC ${FUSEPP_DIR})
set (CMAKE_CXX_STANDARD 17)

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
target_link_libraries(dragonstash fusepp)
target_include_directories(dragonstash PRIVATE include)
