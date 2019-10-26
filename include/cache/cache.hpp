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


class CacheTransactionRO;
class CacheTransactionRW;


class CacheDatabase {
public:
    explicit CacheDatabase(std::shared_ptr<MDBEnv> env);

private:
    std::shared_ptr<MDBEnv> m_env;
    MDBDbi m_meta_db;
    MDBDbi m_inodes_db;
    MDBDbi m_tree_inode_key_db;
    MDBDbi m_tree_name_key_db;

    size_t m_max_name_length;

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

    [[nodiscard]] inline size_t max_name_length() const
    {
        return m_max_name_length;
    }

    [[nodiscard]] Result<void> check_name(std::string_view name, bool for_writing);

};


class Cache {
public:
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
    Result<ino_t> emplace(ino_t parent, std::string_view name,
                          const Backend::Stat &attrs);
};


class CacheTransactionRO {
protected:
    CacheTransactionRO(CacheDatabase &db, MDBROTransaction &&txn);

public:
    /**
     * A group of functions which may be executed when the transaction commits.
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

    template <typename T1, typename T2, typename T3>
    inline void add_commit_hook(T1 &&stage_1_commit,
                                T2 &&stage_1_rollback,
                                T3 &&stage_2_commit)
    {
        m_commit_hooks.emplace_back(CommitHook{
                                        std::forward<T1>(stage_1_commit),
                                        std::forward<T2>(stage_1_rollback),
                                        std::forward<T3>(stage_2_commit),
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
    [[nodiscard]] inline MDBRWTransactionImpl *rw_transaction() {
        // static_cast is safe here, because the constructor of this class
        // only accepts MDBRWTransaction objects.
        return static_cast<MDBRWTransactionImpl*>(ro_transaction());
    }

    [[nodiscard]] ino_t allocate_next_inode();

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
    Result<ino_t> emplace(ino_t parent, std::string_view name,
                          const Backend::Stat &attrs);
};

}

#endif
