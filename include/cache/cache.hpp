#ifndef DRAGONSTASH_CACHE_CACHE_H
#define DRAGONSTASH_CACHE_CACHE_H

#include <sys/types.h>
#include <cstdint>
#include <filesystem>

#include "error.hpp"

#include "lmdb-safe.hh"
#include "backend.hpp"

#include "cache/inode.hpp"

namespace Dragonstash {

static constexpr std::size_t CACHE_PAGE_SIZE = 4096;
static constexpr std::size_t CACHE_INODE_SIZE = CACHE_PAGE_SIZE / 16;

using ino_t = std::uint64_t;

static constexpr ino_t ROOT_INO = 1;

enum DataPriority {
    READAHEAD = 0,
    REQUESTED = 1,
    WRITTEN = 2,
};

struct DirEntry {
    std::string name;
    std::uint64_t ino;
    std::uint32_t mode;
};

class CachedDir {
public:
    explicit CachedDir(MDBROTransaction &&transaction,
                       MDBROCursor &&cursor,
                       ino_t ino);
    CachedDir(const CachedDir &src) = delete;
    CachedDir(CachedDir &&src) noexcept;
    CachedDir &operator=(const CachedDir &src) = delete;
    CachedDir &operator=(CachedDir &&src) noexcept;

private:
    MDBROTransaction m_transaction;
    MDBROCursor m_cursor;
    ino_t m_ino;
    bool m_eof;
    off_t m_size;
    off_t m_curr_off;

    void mark_eof();

    Result<DirEntry> parse_entry(const char *buf, size_t sz);
    Result<DirEntry> parse_entry_v1(const char *buf, size_t sz);

public:
    /**
     * @brief Read a single directory entry from the current offset.
     *
     * @return The directory entry and its offset.
     */
    Result<std::tuple<DirEntry, off_t>> ReadDir();

    /**
     * @brief Set the cursor position to a specific offset.
     * @param offset The offset position.
     *
     * @return Error code if offset is out of bounds.
     */
    Result<void> Seek(off_t offset);

};


class Cache {
public:
    explicit Cache(const std::filesystem::path &db_path);

private:
    std::shared_ptr<MDBEnv> m_env;
    MDBDbi m_inodes_db;
    MDBDbi m_tree_db;

    Result<ino_t> lookup(MDBRWTransaction &transaction,
                         ino_t parent,
                         const char *name);

    Result<std::unique_ptr<CachedDir>> openDir(MDBRWTransaction);

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
    Result<ssize_t> PutData(ino_t ino, const char *data, size_t size,
                            off_t offset, DataPriority prio);

    /**
     * @brief Load data from a file inode from the cache.
     * @param ino
     * @param out
     * @param size
     * @param offset
     * @return
     */
    Result<ssize_t> FetchData(ino_t ino, char *out, size_t size, off_t offset);

    /**
     * @brief Fetch the symlink destination of a link from the cache.
     * @param ino
     * @return
     */
    Result<std::string> FetchLink(ino_t ino);

    /**
     * @brief Store the symlink destination of a link in the cache.
     * @param ino
     * @param dest
     * @return
     */
    Result<void> PutLink(ino_t ino, const char *dest);

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
    Result<void> PutAttr(ino_t ino, const Backend::Stat &data);

    Result<ino_t> PutAttr(ino_t parent, const char *name, const Backend::Stat &data);

    /**
     * @brief Delete an inode and its data from the cache entirely.
     * @param ino
     *
     * In case of directory inodes, this will also delete all contained inodes
     * recursively.
     *
     * @return
     */
    Result<void> Delete(ino_t ino);

    /**
     * @brief Find the inode number for a node starting at parent.
     * @param parent Inode number of the parent; may be ROOT_INO to start at the
     *   root.
     * @param path
     *
     * @return
     */
    Result<ino_t> Lookup(ino_t parent, const char *path);

    /**
     * @brief GetAttr
     * @param ino
     * @return
     */
    Result<Inode> GetAttr(ino_t ino);

    /**
     * @brief Open a snapshot of a directory for reading.
     * @param ino
     *
     * The stream will be consistent even if concurrent deletions/additions
     * happen. It may return entries which have been deleted in the meantime.
     *
     * Instanced of CachedDir should not be kept beyond handling a single
     * fuse callback, because only a limited number of them can co-exist at
     * the same time.
     *
     * @return
     */
    Result<std::unique_ptr<CachedDir>> OpenDir(ino_t ino);

};

}

#endif
