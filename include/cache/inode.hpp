#ifndef DRAGONSTASH_CACHE_INODE_H
#define DRAGONSTASH_CACHE_INODE_H

#include <cstdint>
#include <sys/types.h>

#include "error.hpp"
#include "backend.hpp"

namespace Dragonstash {

struct Inode {
    std::uint32_t mode;
    std::uint64_t size;
    std::uint64_t nblocks;
    std::uint32_t uid;
    std::uint32_t gid;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;

    static constexpr std::size_t serialized_size =
            sizeof(std::uint8_t) + /* version */
            sizeof(mode) +
            sizeof(size) +
            sizeof(nblocks) +
            sizeof(uid) +
            sizeof(gid) +
            sizeof(atime.tv_sec) +
            sizeof(atime.tv_nsec) +
            sizeof(mtime.tv_sec) +
            sizeof(mtime.tv_nsec) +
            sizeof(ctime.tv_sec) +
            sizeof(ctime.tv_nsec);

    static Inode from_backend_stat(const Backend::Stat &data) {
        return Inode{
            .mode = data.mode,
                    .size = data.size,
                    .nblocks = 0,
                    .uid = data.uid,
                    .gid = data.gid,
                    .atime = timespec{data.atime.tv_sec, data.atime.tv_nsec},
                    .mtime = timespec{data.mtime.tv_sec, data.mtime.tv_nsec},
                    .ctime = timespec{data.ctime.tv_sec, data.ctime.tv_nsec},
        };
    }

    static Result<Inode> parse(const std::uint8_t *buf, size_t sz);
    void serialize(std::uint8_t *buf);

private:
    static Result<Inode> parse_v1(const std::uint8_t *buf, size_t sz);
};

}

#endif
