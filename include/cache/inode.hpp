#ifndef DRAGONSTASH_CACHE_INODE_H
#define DRAGONSTASH_CACHE_INODE_H

#include <cstdint>
#include <sys/types.h>
#include <sys/stat.h>

#include "error.hpp"
#include "backend.hpp"
#include "cache/common.hpp"

namespace Dragonstash {

using ino_t = std::uint64_t;

static constexpr ino_t INVALID_INO = 0;
static constexpr ino_t ROOT_INO = 1;

struct CommonFileAttributes {
    std::uint64_t size;
    std::uint64_t nblocks;
    std::uint32_t uid;
    std::uint32_t gid;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
};

struct InodeAttributes: public CommonFileAttributes {
    std::uint32_t mode;
};

struct Stat: public InodeAttributes {
    ino_t ino;

    inline operator struct stat() const {
        struct stat result{};
        result.st_ino = ino;
        result.st_mode = mode;
        result.st_nlink = 1;
        result.st_uid = uid;
        result.st_gid = gid;
        result.st_size = off_t(size);
        result.st_blksize = CACHE_PAGE_SIZE;
        result.st_blocks = blkcnt_t(nblocks);
        result.st_atim = atime;
        result.st_mtim = mtime;
        result.st_ctim = ctime;
        return result;
    }
};

struct DirectoryEntry: public Stat {
    std::string name;
    bool complete;
};

struct Inode: public InodeAttributes {
    ino_t parent;

    static constexpr std::size_t serialized_size =
            sizeof(std::uint8_t) + /* version */
            sizeof(parent) +
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
            InodeAttributes{
                CommonFileAttributes{
                    .size = data.size,
                    .nblocks = 0,
                    .uid = data.uid,
                    .gid = data.gid,
                    .atime = timespec{data.atime.tv_sec, data.atime.tv_nsec},
                    .mtime = timespec{data.mtime.tv_sec, data.mtime.tv_nsec},
                    .ctime = timespec{data.ctime.tv_sec, data.ctime.tv_nsec},
                },
                .mode = data.mode,
            },
            .parent = INVALID_INO,
        };
    }

    static Result<Inode> parse(const std::uint8_t *buf, size_t sz);
    void serialize(std::uint8_t *buf);

private:
    static Result<Inode> parse_v1(const std::uint8_t *buf, size_t sz);
};

}

#endif
