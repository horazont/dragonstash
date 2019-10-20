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

# MAIN THING

set(DRAGONSTASH_SRCS
    src/main.cpp)

add_executable(dragonstash ${DRAGONSTASH_SRCS})
target_link_libraries(dragonstash fusepp)
