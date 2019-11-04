#ifndef DRAGONSTASH_CACHE_COMMON_H
#define DRAGONSTASH_CACHE_COMMON_H

#include <cstdint>

namespace Dragonstash {

static constexpr std::size_t CACHE_PAGE_SIZE = 4096;
static constexpr std::size_t CACHE_INODE_SIZE = CACHE_PAGE_SIZE / 16;

}

#endif
