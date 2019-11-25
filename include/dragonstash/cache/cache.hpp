/**********************************************************************
File name: cache.hpp
This file is part of: DragonStash

LICENSE

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.

FEEDBACK & QUESTIONS

For feedback and questions about DragonStash please e-mail one of the
authors named in the AUTHORS file.
**********************************************************************/
#ifndef DRAGONSTASH_CACHE_CACHE_H
#define DRAGONSTASH_CACHE_CACHE_H

#include <sys/types.h>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <list>

#include "dragonstash/error.hpp"

#include "lmdb-safe.hh"
#include "dragonstash/backend.hpp"
#include "dragonstash/debug_mutex.hpp"

#include "dragonstash/cache/inode.hpp"
#include "dragonstash/cache/common.hpp"

namespace Dragonstash {

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

public:
    InodeReferences() = default;
    InodeReferences(const InodeReferences &src) = delete;
    InodeReferences(InodeReferences &&src) = delete;
    InodeReferences &operator=(const InodeReferences &src) = delete;
    InodeReferences &operator=(InodeReferences &&src) = delete;
    ~InodeReferences() = default;

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
     * If an entry with the same name exists already and the format of the
     * inode (as specified in the mode filed of the attrs) differs, a new entry
     * is created.
     *
     * If the format is equal, the existing inode is updated and the existing
     * inode number is returned.
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


/**
 * @brief A group of functions which may be executed when the transaction
 * commits or aborts.
 *
 * @see CacheTransactionRO::add_transaction_hook for semantics.
 */
class TransactionHook
{
public:
    TransactionHook() = default;
    template <typename T1, typename T2, typename T3, typename T4>
    TransactionHook(T1 &&stage_1_commit,
                    T2 &&stage_1_rollback,
                    T3 &&stage_2_commit,
                    T4 &&rollback):
        m_stage_1_commit(std::forward<T1>(stage_1_commit)),
        m_stage_1_rollback(std::forward<T2>(stage_1_rollback)),
        m_stage_2_commit(std::forward<T3>(stage_2_commit)),
        m_rollback(std::forward<T4>(rollback))
    {

    }
    TransactionHook(const TransactionHook &src) = delete;
    TransactionHook(TransactionHook &&src) = default;
    TransactionHook &operator=(const TransactionHook &src) = delete;
    TransactionHook &operator=(TransactionHook &&src) = default;
    ~TransactionHook() = default;

private:
    /**
     * @brief Executed on commit
     *
     * @see CacheTransactionRO::add_transaction_hook for semantics.
     */
    std::function<Result<void>()> m_stage_1_commit;

    /**
     * @brief Executed when a later commit hook failed
     *
     * @see CacheTransactionRO::add_transaction_hook for semantics.
     */
    std::function<void()> m_stage_1_rollback;

    /**
     * @brief Executed when all pre commit hooks have passed.
     *
     * @see CacheTransactionRO::add_transaction_hook for semantics.
     */
    std::function<void()> m_stage_2_commit;

    /**
     * @brief Executed when the transaction is rolled back.
     *
     * @see CacheTransactionRO::add_transaction_hook for semantics.
     */
    std::function<void()> m_rollback;

    bool m_stage_1_ran{false};

public:
    [[nodiscard]] inline Result<void> stage_1_commit() {
        if (m_stage_1_commit) {
            auto result = m_stage_1_commit();
            if (result) {
                m_stage_1_ran = true;
            }
            return result;
        }
        return make_result();
    }

    inline void stage_1_rollback() {
        if (m_stage_1_ran && m_stage_1_rollback) {
            m_stage_1_rollback();
        }
    }

    inline void stage_2_commit() {
        if (m_stage_2_commit) {
            m_stage_2_commit();
        }
    }

    inline void rollback() {
        if (m_rollback) {
            m_rollback();
        }
    }

};


class CacheTransactionRO {
protected:
    CacheTransactionRO(CacheDatabase &db, MDBROTransaction &&txn,
                       CacheTransactionRW *parent = nullptr);

public:
    CacheTransactionRO(const CacheTransactionRO &src) = delete;
    CacheTransactionRO(CacheTransactionRO &&src) noexcept = default;
    CacheTransactionRO &operator=(const CacheTransactionRO &src) = delete;
    CacheTransactionRO &operator=(CacheTransactionRO &&src) noexcept = default;
    ~CacheTransactionRO() = default;

private:
    CacheDatabase *m_db;
    MDBROTransaction m_txn;
    CacheTransactionRW *m_parent;
    std::vector<TransactionHook> m_transaction_hooks;
    std::unique_lock<debug_mutex> m_inode_counter_lock;

protected:
    [[nodiscard]] inline CacheDatabase &db() {
        return *m_db;
    }

    [[nodiscard]] inline InodeReferences &inode_in_memory_locks();

    [[nodiscard]] inline MDBROTransactionImpl *ro_transaction() {
        return m_txn.get();
    }

protected:
    std::unique_ptr<std::set<ino_t>> m_rewrite_inode_set;

public:
    /**
     * @brief Add a hook to the transaction.
     *
     * The different callbacks of transaction hooks are called during abortion
     * or commit of a transaction.
     */
    template <typename T>
    inline void add_transaction_hook(T &&src)
    {
        m_transaction_hooks.emplace_back(std::forward<T>(src));
    }

    /**
     * @brief Add a new transaction hook.
     *
     * @param stage_1_commit Called during commit()
     * @param stage_1_rollback Called during commit() if a later transaction
     *   hook failed its stage 1 check
     * @param stage_2_commit Called during commit() after all transaction hooks
     *   have passed their stage 1 check
     * @param rollback Called during abort() and called during commit() if any
     *   transaction has failed its stage 1 check.
     *
     * Any callback can be nullptr, in which case it is assumed that it succeds.
     *
     * For a single TransactionHook, the commit flow is the following:
     *
     * 1. Call stage_1_commit. If the result is ok, continue with the next point
     *    below.
     *
     *    If the result is *not* ok, the following happens:
     *    1. All previous transaction hooks have their stage_1_rollback called
     *       in reverse order.
     *    2. All transaction hooks have their rollback called in reverse order.
     *    3. The transaction is aborted and the error code of the first failing
     *       hook is returned.
     *
     * 2. Call stage_2_commit. This must not fail.
     *
     * The CacheTransactionRO class gives the following guarantees:
     *
     * - stage_1_commit has been called exactly once for a transaction which
     *   committed successfully
     * - stage_2_commit has been called exactly once for a transaction which
     *   committed successfully
     * - if stage_1_commit is called, either stage_2_commit or stage_1_rollback
     *   is called
     * - if stage_1_rollback is called, rollback is called after all other
     *   transaction hooks also had their stage_1_rollback called
     * - transaction hooks are evaluated in order for commit callbacks and
     *   in reverse order for rollback callbacks
     * - if stage_2_commit is called, all stage_1_commit callbacks of all
     *   transaction hooks have executed successfully.
     */
    template <typename T1, typename T2, typename T3, typename T4>
    inline void add_transaction_hook(T1 &&stage_1_commit,
                                     T2 &&stage_1_rollback,
                                     T3 &&stage_2_commit,
                                     T4 &&rollback)
    {
        m_transaction_hooks.emplace_back(std::forward<T1>(stage_1_commit),
                                         std::forward<T2>(stage_1_rollback),
                                         std::forward<T3>(stage_2_commit),
                                         std::forward<T4>(rollback));
    }

    /**
     * @brief Add a new transaction hook which only uses commit callbacks.
     *
     * @see add_transaction_hook
     */
    template <typename T1, typename T2, typename T3>
    inline void add_commit_hook(T1 &&stage_1_commit,
                                T2 &&stage_1_rollback,
                                T3 &&stage_2_commit)
    {
        add_transaction_hook(std::forward<T1>(stage_1_commit),
                             std::forward<T2>(stage_1_rollback),
                             std::forward<T3>(stage_2_commit),
                             nullptr);
    }

    /**
     * @brief Add a new transaction hook which only uses the rollback callback.
     *
     * @see add_transaction_hook
     */
    template <typename T1>
    inline void add_rollback_hook(T1 &&rollback) {
        add_transaction_hook(nullptr, nullptr, nullptr,
                             std::forward<T1>(rollback));
    }

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

    [[nodiscard]] Result<bool> test_flag(ino_t ino, InodeFlag flag);

    inline explicit operator bool() const {
        return bool(m_txn);
    }

    void abort();
    [[nodiscard]] Result<void> commit();

    friend class Cache;
};

class CacheTransactionRW: public CacheTransactionRO {
protected:
    CacheTransactionRW(CacheDatabase &cache, MDBRWTransaction &&txn,
                       CacheTransactionRW *parent = nullptr);

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
        return CacheTransactionRW(db(), rw_transaction()->getRWTransaction(), this);
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

    [[nodiscard]] Result<void> update_flags(ino_t ino,
                                            std::initializer_list<InodeFlag> to_set,
                                            std::initializer_list<InodeFlag> to_clear = std::initializer_list<InodeFlag>());

    /**
     * @brief Start rewriting a complete directory
     *
     * @param dir The inode of the directory to rewrite.
     *
     * This marks all inodes in the directory for removal, but does not unlink
     * them yet. The emplace() operation can be used to remove the mark for
     * removal.
     *
     * Calling finish_dir_rewrite() unlinks all inodes from the directory which
     * are marked for removal. Calling finish_dir_rewrite() right after
     * start_dir_rewrite() can be used as a way to clear a directory completely.
     *
     * @note start and finish of the rewrite operation must happen in the same
     * transaction; it is not possible to start a rewrite operation in a
     * subtransaction and finish it in the parent transaction or vice versa.
     *
     * Error codes:
     *
     * - ENOTDIR: @a dir does not refer to a directory.
     * - ENOENT: @a dir does not exist.
     * - EALREADY: if a rewrite operation is already in progress in this
     *   transaction
     */
    [[nodiscard]] Result<void> start_dir_rewrite(ino_t dir);

    /**
     * @brief Complete a directory rewrite operation.
     *
     * @see start_dir_rewrite()
     *
     * Error codes:
     *
     * - EBADFD: No rewrite operation is in progress.
     */
    [[nodiscard]] Result<void> finish_dir_rewrite();
};

}

#endif
