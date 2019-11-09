#ifndef DRAGONSTASH_CACHE_CACHE_H
#define DRAGONSTASH_CACHE_CACHE_H

#include <sys/types.h>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <list>

#include "error.hpp"

#include "lmdb-safe.hh"
#include "backend.hpp"
#include "debug_mutex.hpp"

#include "cache/inode.hpp"
#include "cache/common.hpp"

namespace Dragonstash {

enum DataPriority {
    READAHEAD = 0,
    REQUESTED = 1,
    WRITTEN = 2,
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
    ~CachedDir() = default;

private:
    MDBROTransaction m_transaction;
    MDBROCursor m_cursor;
    ino_t m_ino;
    bool m_eof;
    off_t m_size;
    off_t m_curr_off;

    void mark_eof();

    Result<DirectoryEntry> parse_entry(const char *buf, size_t sz);
    Result<DirectoryEntry> parse_entry_v1(const char *buf, size_t sz);

public:
    /**
     * @brief Read a single directory entry from the current offset.
     *
     * @return The directory entry and its offset.
     */
    Result<std::tuple<DirectoryEntry, off_t>> ReadDir();

    /**
     * @brief Set the cursor position to a specific offset.
     * @param offset The offset position.
     *
     * @return Error code if offset is out of bounds.
     */
    Result<void> Seek(off_t offset);

};


class CacheTransactionRO;
class CacheTransactionRW;


class InodeReferences {
public:
    struct Record {
        std::uint64_t nrefs;
        bool doomed;
    };

private:
    std::unordered_map<ino_t, Record> m_refs;

public:
    [[nodiscard]] inline Result<std::uint64_t> incref(ino_t ino, std::uint64_t by) {
        auto iter = m_refs.find(ino);
        if (iter == m_refs.end()) {
            m_refs.emplace(ino, Record{by, false});
            return make_result(by);
        }
        if (iter->second.doomed) {
            return make_result(FAILED, ESTALE);
        }
        iter->second.nrefs += by;
        return make_result(iter->second.nrefs);
    }

    [[nodiscard]] inline Result<std::uint64_t> decref(ino_t ino, std::uint64_t by) {
        if (by == 0) {
            return make_result(FAILED, EINVAL);
        }

        auto iter = m_refs.find(ino);
        if (iter == m_refs.end() || iter->second.nrefs < by) {
            throw std::runtime_error("attempt to decrease counter below zero");
        }
        iter->second.nrefs -= by;
        return make_result(iter->second.nrefs);
    }

    [[nodiscard]] inline Result<void> doom(ino_t ino) {
        auto iter = m_refs.find(ino);
        if (iter == m_refs.end()) {
            m_refs.emplace(ino, Record{0, true});
            return make_result();
        }
        if (iter->second.nrefs > 0) {
            return make_result(FAILED, EBUSY);
        }
        iter->second.doomed = true;
        return make_result();
    }

    [[nodiscard]] inline bool doomed(ino_t ino) const {
        auto iter = m_refs.find(ino);
        if (iter == m_refs.end()) {
            return false;
        }
        return iter->second.doomed;
    }

    [[nodiscard]] inline std::uint64_t refcount(ino_t ino) const {
        auto iter = m_refs.find(ino);
        if (iter == m_refs.end()) {
            return 0;
        }
        return iter->second.nrefs;
    }

};


class CacheDatabase {
public:
    CacheDatabase() = delete;
    explicit CacheDatabase(std::shared_ptr<MDBEnv> env);

private:
    std::shared_ptr<MDBEnv> m_env;
    MDBDbi m_meta_db;
    MDBDbi m_inodes_db;
    MDBDbi m_tree_inode_key_db;
    MDBDbi m_tree_name_key_db;
    MDBDbi m_orphan_db;
    MDBDbi m_links_db;

    size_t m_max_name_length;

    debug_mutex m_in_memory_lock_mutex;
    InodeReferences m_in_memory_locks;

    void validate_max_key_size();

public:
    [[nodiscard]] inline MDBEnv &env()
    {
        return *m_env;
    }

    [[nodiscard]] inline MDBDbi &meta_db()
    {
        return m_meta_db;
    }

    [[nodiscard]] inline MDBDbi &inodes_db()
    {
        return m_inodes_db;
    }

    [[nodiscard]] inline MDBDbi &tree_inode_key_db()
    {
        return m_tree_inode_key_db;
    }

    [[nodiscard]] inline MDBDbi &tree_name_key_db()
    {
        return m_tree_name_key_db;
    }

    [[nodiscard]] inline MDBDbi &orphan_db()
    {
        return m_orphan_db;
    }

    [[nodiscard]] inline MDBDbi &links_db()
    {
        return m_links_db;
    }

    [[nodiscard]] inline size_t max_name_length() const
    {
        return m_max_name_length;
    }

    [[nodiscard]] Result<void> check_name(std::string_view name, bool for_writing);

    [[nodiscard]] inline auto in_memory_lock_guard() {
        return std::unique_lock<debug_mutex>(m_in_memory_lock_mutex);
    }

    [[nodiscard]] inline InodeReferences &in_memory_locks() {
        return m_in_memory_locks;
    }

};


/**
 * @brief Handle to a regular, cached file.
 * 
 * This handle arbitrates access to the cached file. The file can be manipulated
 * concurrently. The effects of that are similar to the effects of concurrently
 * manipulating a file on disk.
 * 
 * Most operations will generally not need a valid write transaction, however,
 * in some cases, (write) access to the LMDB data is required (for example, 
 * when resizing a file due to a write beyond the current last block).  In such
 * cases, if not passed a valid write transaction, the methods will start a 
 * transaction on their own.
 * 
 * Note that there is still no synchronisation between the LMDB-backed metadata
 * and the data inside the cache; in case of a crash, it is possible that 
 * data is missing from the cache for which LMDB already has metadata.
 *
 * Random notes:
 *
 * - Three backing files: page list (vector of startpage+size tuples), blockmap
 *   (two bytes / page with access counter and state), actual data
 * - Idea: use mmaped file with vector directly. however, that will not work
 *   because C++ allocators require zero-initialised memory from allocate() and
 *   do not support reallocate (which would be required to resize the mapping as
 *   needed).
 *   -> we need a custom vector-like implementation
 */
class RegularFileHandle {
private:
    ino_t m_ino;
    int m_blockmap_fd;
    int m_blocks_fd;
    void *m_blockmap_mmap;

public:
    [[nodiscard]] ino_t inode() const;

    [[nodiscard]] Result<std::size_t> pread(off_t off, void *buf, std::size_t n);
    [[nodiscard]] Result<std::size_t> pwrite(off_t off, const void *buf, std::size_t n);
    [[nodiscard]] Result<void> truncate(std::size_t size);
    [[nodiscard]] Result<void> allocate(int mode, off_t offset, off_t len);
    [[nodiscard]] Result<void> fsync();

};


class Cache {
public:
    static const bool deadlock_detection;

public:
    Cache() = delete;
    explicit Cache(const std::filesystem::path &db_path);
    Cache(const Cache &src) = delete;
    Cache(Cache &&src) = delete;
    Cache &operator=(const Cache &src) = delete;
    Cache &operator=(Cache &&src) = delete;
    ~Cache() = default;

private:
    CacheDatabase m_db;

public:
    /**
     * @brief Get maximum length of directory entry names.
     */
    [[nodiscard]] inline size_t max_name_length() const
    {
        return m_db.max_name_length();
    }

    [[nodiscard]] CacheTransactionRO begin_ro();
    [[nodiscard]] CacheTransactionRW begin_rw();

    /**
     * @brief Look up the name of an inode
     * @param ino Number of the inode
     * @return
     */
    [[nodiscard]] Result<std::string> name(ino_t ino);

    /**
     * @brief Look up the parent inode of an inode.
     * @param ino
     * @return
     */
    [[nodiscard]] Result<ino_t> parent(ino_t ino);

    /**
     * @brief Look up the inode number of an entry in a directory.
     * @param parent The inode of the parent directory.
     * @param name Name of the entry to look up.
     * @return The inode number of the entry.
     */
    [[nodiscard]] Result<ino_t> lookup(ino_t parent, std::string_view name);

    [[nodiscard]] Result<Stat> getattr(ino_t ino);

    [[nodiscard]] Result<void> setattr(ino_t ino, const Stat &attrs);

    /**
     * @brief Create or replace a directory entry.
     * @param parent Inode of the directory to operate in.
     * @param name Name of the directory entry.
     * @param attrs Attributes of the new entry.
     *
     * If an entry with the same name exists already, the old one is replaced.
     *
     * @return The inode of the new entry.
     */
    [[nodiscard]] Result<ino_t> emplace(ino_t parent, std::string_view name,
                          const InodeAttributes &attrs);

    /**
     * @brief Increase the lock counter on an inode
     * @param ino Affected inode
     *
     * An inode with a lock counter greater than zero cannot be removed from
     * the cache. The inode can be removed from the directory structure (and
     * thus become orphaned), but it will stay in the storage until the lock
     * count has reduced to zero.
     *
     * Lock counters are not persisted to disk.
     */
    [[nodiscard]] Result<void> lock(ino_t ino);

    [[nodiscard]] Result<void> release(ino_t ino);

    [[nodiscard]] Result<std::string> readlink(ino_t ino);

    [[nodiscard]] Result<void> writelink(ino_t ino, std::string_view dest);

    [[nodiscard]] Result<std::string> path(ino_t ino);
};


class CacheTransactionRO {
protected:
    CacheTransactionRO(CacheDatabase &db, MDBROTransaction &&txn);

public:
    /**
     * A group of functions which may be executed when the transaction commits
     * or aborts.
     */
    struct CommitHook
    {
        /**
         * @brief Executed on commit
         *
         * If the function returns a non-success result, all previously
         * executed CommitHooks will have their pre_rollback function called and
         * the transaction will abort instead of commit.
         *
         * The result is returned from the commit method.
         */
        std::function<Result<void>()> stage_1_commit;

        /**
         * @brief Executed when a later commit hook failed
         *
         * This is to allow a commit hook to roll back changes which it has
         * prepared to commit if later commit hooks have failed.
         */
        std::function<void()> stage_1_rollback;

        /**
         * @brief Executed when all pre commit hooks have passed.
         *
         * This function must not fail, otherwise the results will be
         * inconsistent.
         */
        std::function<void()> stage_2_commit;

        /**
         * @brief Executed when the transaction is rolled back.
         */
        std::function<void()> rollback;
    };

public:
    CacheTransactionRO(const CacheTransactionRO &src) = delete;
    CacheTransactionRO(CacheTransactionRO &&src) noexcept = default;
    CacheTransactionRO &operator=(const CacheTransactionRO &src) = delete;
    CacheTransactionRO &operator=(CacheTransactionRO &&src) noexcept = default;
    ~CacheTransactionRO() = default;

private:
    CacheDatabase *m_db;
    MDBROTransaction m_txn;
    std::vector<CommitHook> m_commit_hooks;
    std::list<MDBROTransaction> m_subtxns;

protected:
    std::unique_lock<debug_mutex> m_inode_counter_lock;

protected:
    [[nodiscard]] inline CacheDatabase &db() {
        return *m_db;
    }

    [[nodiscard]] inline MDBROTransactionImpl *ro_transaction() {
        return m_txn.get();
    }

    inline void add_commit_hook(CommitHook &&src)
    {
        m_commit_hooks.emplace_back(std::move(src));
    }

    template <typename T1, typename T2, typename T3, typename T4>
    inline void add_commit_hook(T1 &&stage_1_commit,
                                T2 &&stage_1_rollback,
                                T3 &&stage_2_commit,
                                T4 &&rollback)
    {
        m_commit_hooks.emplace_back(CommitHook{
                                        std::forward<T1>(stage_1_commit),
                                        std::forward<T2>(stage_1_rollback),
                                        std::forward<T3>(stage_2_commit),
                                        std::forward<T4>(rollback),
                                    });
    }

public:
    /**
     * @brief Look up the name of an inode in a directory
     * @param parent Number of the parent inode
     * @param ino Number of the inode
     * @return
     */
    [[nodiscard]] Result<std::string> name(ino_t parent, ino_t ino);

    /**
     * @brief Look up the name of an inode
     * @param ino Number of the inode
     *
     * This is a convenience wrapper which uses parent() and then name().
     *
     * @return
     */
    [[nodiscard]] Result<std::string> name(ino_t ino);

    /**
     * @brief Look up the parent inode of an inode.
     * @param ino
     * @return
     */
    [[nodiscard]] Result<ino_t> parent(ino_t ino);

    /**
     * @brief Look up the inode number of an entry in a directory.
     * @param parent The inode of the parent directory.
     * @param name Name of the entry to look up.
     * @return The inode number of the entry.
     */
    [[nodiscard]] Result<ino_t> lookup(ino_t parent, std::string_view name);

    [[nodiscard]] Result<Stat> getattr(ino_t ino);

    [[nodiscard]] Result<std::string> readlink(ino_t ino);

    /**
     * @brief Read a single directory entry from a directory.
     *
     * @param prev_end The last inode returned by readdir to continue reading
     * the directory stream or zero to start from the beginning.
     * @return error code zero at EOF
     *
     * For all directories except the root directory, this also emits the
     * dot and dotdot entries. For the root directory, only the dot entry is
     * emitted.
     *
     * In the future, dotdot may also be returned for the root inode.
     */
    [[nodiscard]] Result<DirectoryEntry> readdir(ino_t dir, ino_t prev_end);

    /**
     * @brief Reconstruct the full path of an inode.
     */
    [[nodiscard]] Result<std::string> path(ino_t ino);

    /**
     * @brief Increase reference counter of an inode by one.
     *
     * Error Codes:
     *
     * - ESTALE: The inode has been deleted by a future transaction and
     *   returning a lock is not safely possible.
     * - ENOENT: No such inode.
     */
    [[nodiscard]] Result<void> lock(ino_t ino);

    /**
     * @brief Decrease the reference counter of an inode.
     *
     * @note The inode is not deleted immediately even if it is orphaned and the
     * reference counter drops to zero if this is a read-only transaction. If
     * this is a read-write transaction, the inode may or may not be deleted
     * immediately.
     */
    [[nodiscard]] Result<void> release(ino_t ino, uint64_t nlocks = 1);

    inline explicit operator bool() const {
        return bool(m_txn);
    }

    void abort();
    [[nodiscard]] Result<void> commit();

    friend class Cache;
};

class CacheTransactionRW: public CacheTransactionRO {
protected:
    CacheTransactionRW(CacheDatabase &cache, MDBRWTransaction &&txn);

public:
    CacheTransactionRW(const CacheTransactionRW &src) = delete;
    CacheTransactionRW(CacheTransactionRW &&src) noexcept = default;
    CacheTransactionRW &operator=(const CacheTransactionRW &src) = delete;
    CacheTransactionRW &operator=(CacheTransactionRW &&src) noexcept = default;
    ~CacheTransactionRW() = default;

private:
    // TODO: think about how we can properly nest RW transactions internally.
    // Most importantly, we need correct handling of CommitHooks in case the
    // nested transaction aborts/commits; the commit hooks have externally
    // visible side-effects which should not become visible before the outermost
    // transaction completes; however, at the same time, they are part of the
    // inner transaction and I'm not sure if they should be checked beforehands?
    // In addition, we need to transfer the inode lock to the parent in any
    // case.
    [[nodiscard]] inline MDBRWTransactionImpl *rw_transaction() {
        // static_cast is safe here, because the constructor of this class
        // only accepts MDBRWTransaction objects.
        return static_cast<MDBRWTransactionImpl*>(ro_transaction());
    }

    [[nodiscard]] ino_t allocate_next_inode();

    [[nodiscard]] Result<void> make_orphan(ino_t ino);

public:
    [[nodiscard]] inline CacheTransactionRW begin_nested()
    {
        return CacheTransactionRW(db(), rw_transaction()->getRWTransaction());
    }

    [[nodiscard]] inline CacheTransactionRO begin_nested_ro()
    {
        return begin_nested();
    }

    friend class Cache;

    /**
     * @brief Create or replace a directory entry.
     * @param parent Inode of the directory to operate in.
     * @param name Name of the directory entry.
     * @param attrs Attributes of the new entry.
     *
     * If an entry with the same name exists already, the old one is replaced.
     *
     * @return The inode of the new entry.
     */
    [[nodiscard]] Result<ino_t> emplace(ino_t parent, std::string_view name,
                                        const InodeAttributes &attrs);

    [[nodiscard]] Result<void> unlink(ino_t ino);
    [[nodiscard]] Result<void> unlink(ino_t parent, ino_t child);
    [[nodiscard]] Result<void> unlink(ino_t parent, std::string_view name);

    // TODO: `which` argument
    [[nodiscard]] Result<void> setattr(ino_t ino, const CommonFileAttributes &attrs);

    [[nodiscard]] Result<void> clean_orphans();

    [[nodiscard]] Result<void> writelink(ino_t ino, std::string_view dest);
};

}

#endif
