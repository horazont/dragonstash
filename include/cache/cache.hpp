#ifndef DRAGONSTASH_CACHE_CACHE_H
#define DRAGONSTASH_CACHE_CACHE_H

#include <sys/types.h>
#include <cstdint>

#include "error.hpp"

namespace Dragonstash {

namespace Backend {
struct Stat;
}

static constexpr std::size_t CACHE_PAGE_SIZE = 4096;
static constexpr std::size_t CACHE_INODE_SIZE = CACHE_PAGE_SIZE / 16;

using ino_t = std::uint64_t;

static constexpr ino_t ROOT_INO = 1;

enum DataPriority {
    READAHEAD = 0,
    REQUESTED = 1,
    WRITTEN = 2,
};

struct __attribute__ ((packed)) InodeTimestampV1 {
    uint64_t secs;
    uint32_t nsecs;
};

struct __attribute__ ((packed)) InodeV1 {
    uint32_t mode;
    uint64_t size;
    uint32_t uid;
    uint32_t gid;
    InodeTimestampV1 atime, mtime, ctime;

    uint8_t reserved[CACHE_INODE_SIZE - 56];
};

static_assert(sizeof(InodeV1) == CACHE_INODE_SIZE, "inode size is incorrect");

class Cache {
public:
    explicit Cache(const std::string &db_path);

public:
    /**
     * @brief Store data associated with a file inode into the cache.
     * @param ino
     * @param data
     * @param size
     * @param offset
     * @param prio
     * @return
     */
    Result<ssize_t> PutData(const ino_t ino,
                            const char *data,
                            const size_t size,
                            const off_t offset,
                            const DataPriority prio);

    /**
     * @brief Load data from a file inode from the cache.
     * @param ino
     * @param out
     * @param size
     * @param offset
     * @return
     */
    Result<ssize_t> FetchData(const ino_t ino,
                              char *out,
                              const size_t size,
                              const off_t offset);

    /**
     * @brief Fetch the symlink destination of a link from the cache.
     * @param ino
     * @return
     */
    Result<std::string> FetchLink(const ino_t ino);

    /**
     * @brief Store the symlink destination of a link in the cache.
     * @param ino
     * @param dest
     * @return
     */
    Result<void> PutLink(const ino_t ino, const char *dest);

    /**
     * @brief Update the attributes of an inode in the cache.
     * @param ino
     * @param data
     *
     * This may change the type of the inode. If the inode type changes, all
     * data associated with the inode is discarded. In case of directory inodes,
     * this means that all contained inodes will be deleted from the cache,
     * including directories.
     *
     * @return
     */
    Result<void> SetAttr(const ino_t ino, const Backend::Stat &data);

    /**
     * @brief Delete an inode and its data from the cache entirely.
     * @param ino
     *
     * In case of directory inodes, this will also delete all contained inodes
     * recursively.
     *
     * @return
     */
    Result<void> Delete(const ino_t ino);

    /**
     * @brief Find the inode number for a node starting at parent.
     * @param parent Inode number of the parent; may be ROOT_INO to start at the
     *   root.
     * @param path
     *
     * @return
     */
    Result<ino_t> Lookup(const ino_t parent, const char *path);

};

}

#endif
