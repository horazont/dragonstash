#include "cache/cache.hpp"

#include <cassert>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>

/**
 * LMDB database layout
 *
 * Database `inodes` (MDB_INTEGERKEY):
 *
 * - key: uint64_t inode
 * - value: uint8_t version + struct InodeV1 + type-specific inode data
 *
 * Database `treei`:
 *
 * - key: uint64_t parent_inode + uint64_t child_inode
 * - value: uint32_t mode cache + name
 *
 * Database `treen`:
 *
 * - key: uint64_t parent_inode + name
 * - value: uint64_t child_inode + uint32_t mode cache
 */

namespace Dragonstash {

static const std::string_view DB_NAME_META = "meta";
static const std::string_view DB_NAME_INODES = "inodes";
static const std::string_view DB_NAME_TREE_INODE_KEY = "treei";
static const std::string_view DB_NAME_TREE_NAME_KEY = "treen";

static const std::string_view META_KEY_NEXT_INO = "next_ino";

template<typename T, typename _ = typename std::enable_if<std::is_arithmetic<T>::value && std::numeric_limits<T>::min() == 0>::type>
T safe_dec(T &value, T by = 1)
{
    if (by <= 0) {
        return value;
    }
    if (value < by) {
        throw std::runtime_error("attempt to decrease counter below zero");
    }
    value -= by;
    return value;
}


CachedDir::CachedDir(MDBROTransaction &&transaction,
                     MDBROCursor &&cursor,
                     const ino_t ino):
    m_transaction(std::move(transaction)),
    m_cursor(std::move(cursor)),
    m_ino(ino),
    m_eof(false),
    m_size(-1),
    m_curr_off(0)
{
    MDBInVal key(ino);
    MDBOutVal unused_key{};
    MDBOutVal unused_value{};
    /* position the cursor at next item to read */
    /* if not found, the directory has zero entries */
    if (m_cursor.find(key, unused_key, unused_value) == MDB_NOTFOUND) {
        mark_eof();
    }
}

void CachedDir::mark_eof()
{
    if (m_eof) {
        return;
    }

    m_eof = true;
    assert(m_curr_off + 1 == m_size || m_size == -1);
    m_size = m_curr_off + 1;
}

Result<DirectoryEntry> CachedDir::parse_entry(const char *buf, size_t sz)
{
    if (sz < sizeof(std::uint8_t)) {
        return make_result(FAILED, -EIO);
    }

    std::uint8_t version;
    memcpy(&version, &buf[0], sizeof(version));

    switch (version) {
    case 1:
        return parse_entry_v1(&buf[1], sz-1);
    default:
        return make_result(FAILED, -EIO);
    }
}

Result<DirectoryEntry> CachedDir::parse_entry_v1(const char *buf, size_t sz)
{
    if (sz <= sizeof(std::uint64_t) + sizeof(uint32_t)) {
        return make_result(FAILED, -EIO);
    }

    DirectoryEntry result{};
    result.complete = false;
    memcpy(&result.ino, &buf[0], sizeof(result.ino));
    buf += sizeof(result.ino); sz -= sizeof(result.ino);

    memcpy(&result.mode, &buf[0], sizeof(result.mode));
    buf += sizeof(result.mode); sz -= sizeof(result.mode);

    assert(sz >= 1);
    result.name = std::string(&buf[0], sz);

    return result;
}

Result<std::tuple<DirectoryEntry, off_t> > CachedDir::ReadDir()
{
    if (m_eof) {
        /* return zero error code for EOF */
        return make_result(FAILED, 0);
    }

    MDBOutVal key, value;
    const off_t this_offset = m_curr_off;
    m_cursor.get(key, value, MDB_GET_CURRENT);
    assert(key.get<ino_t>() == m_ino);

    /* check if the next item still belongs to this key */
    if (m_cursor.nextprev(key, value, MDB_NEXT_DUP) == MDB_NOTFOUND) {
        mark_eof();
    } else {
        /* this is an assumption about how LMDB works */
        assert(key.get<ino_t>() == m_ino);
        m_curr_off += 1;
    }


    Result<DirectoryEntry> result =
            parse_entry(reinterpret_cast<char*>(value.d_mdbval.mv_data),
                        value.d_mdbval.mv_size);
    if (!result) {
        return make_result(FAILED, result.error());
    }

    return std::make_tuple(std::move(*result), this_offset);
}

Result<void> CachedDir::Seek(off_t offset)
{
    if (offset < 0) {
        return make_result(FAILED, -EINVAL);
    }
    if (offset > m_curr_off) {
        /* seeking forward is not supported for now; we should not need this
         * since directories are supposed to be a stream */
        return make_result(FAILED, -EINVAL);
    }
    if (offset == m_curr_off) {
        return make_result();
    }

    MDBOutVal key, value;
    while (m_curr_off > offset) {
        int rc = m_cursor.nextprev(key, value, MDB_PREV_DUP);
        assert(rc == 0);
        assert(key.get<ino_t>() == m_ino);
        m_curr_off -= 1;
    }

    return make_result();
}


/* Dragonstash::CacheDatabase */

CacheDatabase::CacheDatabase(std::shared_ptr<MDBEnv> env):
    m_env(std::move(env)),
    m_meta_db(m_env->openDB(DB_NAME_META, MDB_CREATE)),
    m_inodes_db(m_env->openDB(DB_NAME_INODES, MDB_CREATE)),
    m_tree_inode_key_db(m_env->openDB(DB_NAME_TREE_INODE_KEY, MDB_CREATE)),
    m_tree_name_key_db(m_env->openDB(DB_NAME_TREE_NAME_KEY, MDB_CREATE)),
    m_max_name_length(0)
{
    validate_max_key_size();
}

void CacheDatabase::validate_max_key_size()
{
    const size_t max_key_size = mdb_env_get_maxkeysize(*m_env);
    /* at least one byte for directory entry names */
    if (max_key_size < sizeof(ino_t) + 1) {
        throw std::runtime_error("cannot use this version of LMDB. maxkeysize too small.");
    }
    m_max_name_length = max_key_size - sizeof(ino_t);
}

Result<void> CacheDatabase::check_name(std::string_view name, bool for_writing)
{
    if (name.length() > m_max_name_length) {
        return make_result(FAILED, -ENAMETOOLONG);
    }
    if (!for_writing) {
        return make_result();
    }

    // more thorough checks before persisting a name into the db
    for (char ch: name) {
        switch (ch) {
        case 0:
        case '/':
            return make_result(FAILED, -EINVAL);
        default:;
        }
    }
    return make_result();
}


/* Dragonstash::Cache */

Cache::Cache(const std::filesystem::path &db_path):
    m_db(getMDBEnv((db_path / "db").c_str(), MDB_NOSUBDIR, 0700))
{
    auto txn = m_db.env().getRWTransaction();
    MDBOutVal value{};
    if (txn->get(m_db.meta_db(), META_KEY_NEXT_INO, value) == MDB_NOTFOUND) {
        // initialise next inode to root inode + 1
        const ino_t next_inode = ROOT_INO + 1;
        txn->put(m_db.meta_db(), META_KEY_NEXT_INO, next_inode);

        // initialise the remainder of the database
        // create root inode!
        struct timespec now{};
        clock_gettime(CLOCK_REALTIME, &now);
        Inode root{
            InodeAttributes{
                CommonFileAttributes{
                    .uid = getuid(),
                    .gid = getgid(),
                    .atime = now,
                    .mtime = now,
                    .ctime = now,
                },
                .mode = S_IFDIR,
            },
            .parent = INVALID_INO,
        };
        const ino_t root_ino = ROOT_INO;
        std::string buf;
        buf.resize(Inode::serialized_size);
        root.serialize(reinterpret_cast<std::uint8_t*>(buf.data()));
        txn->put(m_db.inodes_db(), root_ino, buf);
    }

    txn->commit();
}

CacheTransactionRO Cache::begin_ro()
{
    return CacheTransactionRO(m_db, m_db.env().getROTransaction());
}

CacheTransactionRW Cache::begin_rw()
{
    return CacheTransactionRW(m_db, m_db.env().getRWTransaction());
}

Result<std::string> Cache::name(ino_t ino)
{
    return begin_ro().name(ino);
}

Result<ino_t> Cache::parent(ino_t ino)
{
    return begin_ro().parent(ino);
}

Result<ino_t> Cache::lookup(ino_t parent, std::string_view name)
{
    return begin_ro().lookup(parent, name);
}

Result<Stat> Cache::getattr(ino_t ino)
{
    return begin_ro().getattr(ino);
}

template<typename T>
auto with_rw_txn(CacheTransactionRW &&txn, T &&f) -> decltype(f(txn))
{
    auto result = f(txn);
    if (!result) {
        txn.abort();
        return result;
    }
    {
        auto commit_result = txn.commit();
        if (!commit_result) {
            return make_result(FAILED, commit_result.error());
        }
    }
    return result;
}

Result<ino_t> Cache::emplace(ino_t parent, std::string_view name, const InodeAttributes &attrs)
{
    return with_rw_txn(begin_rw(), [parent, name, &attrs](CacheTransactionRW &txn){
        return txn.emplace(parent, name, attrs);
    });
}

Result<void> Cache::lock(ino_t ino)
{
    return with_rw_txn(begin_rw(), [ino](CacheTransactionRW &txn){
        return txn.lock(ino);
    });
}

Result<void> Cache::release(ino_t ino)
{
    return with_rw_txn(begin_rw(), [ino](CacheTransactionRW &txn){
        return txn.release(ino);
    });
}

/* Dragonstash::CacheTransactionRO */

CacheTransactionRO::CacheTransactionRO(CacheDatabase &db, MDBROTransaction &&txn):
    m_db(&db),
    m_txn(std::move(txn))
{

}

Result<std::string> CacheTransactionRO::name(ino_t parent, ino_t ino)
{
    if (ino == ROOT_INO || parent == INVALID_INO) {
        return make_result(std::string_view(""));
    }

    std::string key;
    key.resize(sizeof(ino_t)*2);
    memcpy(&key[0], &parent, sizeof(ino_t));
    memcpy(&key[sizeof(ino_t)], &ino, sizeof(ino_t));

    auto cursor = ro_transaction()->getROCursor(db().tree_inode_key_db());
    MDBOutVal key_out{};
    MDBOutVal value{};
    if (int rc = cursor.find(key, key_out, value) == MDB_NOTFOUND) {
        // should this be possible? on deletion, we should normally unset the
        // parent...
        return make_result(FAILED, -ENOENT);
    }

    std::string name(reinterpret_cast<char*>(value.d_mdbval.mv_data),
                     value.d_mdbval.mv_size);
    return make_result(name);
}

Result<std::string> CacheTransactionRO::name(ino_t ino)
{
    if (ino == ROOT_INO) {
        return make_result(std::string_view(""));
    }

    auto parent_result = parent(ino);
    if (!parent_result) {
        return make_result(FAILED, parent_result.error());
    }
    if (*parent_result == INVALID_INO) {
        return "";
    }
    return name(*parent_result, ino);
}

Result<ino_t> CacheTransactionRO::parent(ino_t ino)
{
    MDBOutVal value{};
    if (int rc = ro_transaction()->get(db().inodes_db(), ino, value) == MDB_NOTFOUND) {
        return make_result(FAILED, -ENOENT);
    }

    auto parsed = Inode::parse(reinterpret_cast<std::uint8_t*>(value.d_mdbval.mv_data),
                               value.d_mdbval.mv_size);
    if (!parsed) {
        return make_result(FAILED, parsed.error());
    }

    return parsed->parent;
}

Result<ino_t> CacheTransactionRO::lookup(ino_t parent, std::string_view name)
{
    {
        Result<void> name_ok = db().check_name(name, false);
        if (!name_ok) {
            return make_result(FAILED, name_ok.error());
        }
    }

    if (parent == INVALID_INO) {
        return make_result(FAILED, -EINVAL);
    }

    auto cursor = ro_transaction()->getROCursor(m_db->tree_name_key_db());

    MDBOutVal key_out{};
    MDBOutVal value_out{};

    for (int rc = cursor.lower_bound(parent, key_out, value_out);
         rc != MDB_NOTFOUND;
         rc = cursor.nextprev(key_out, value_out, MDB_NEXT))
    {
        assert(key_out.d_mdbval.mv_size >= sizeof(ino_t));
        assert(value_out.d_mdbval.mv_size == sizeof(ino_t));

        const size_t name_length = key_out.d_mdbval.mv_size - sizeof(ino_t);
        if (name_length != name.length()) {
            continue;
        }

        ino_t entry_parent_ino;
        memcpy(&entry_parent_ino, key_out.d_mdbval.mv_data, sizeof(ino_t));
        if (entry_parent_ino != parent) {
            // end of directory
            break;
        }

        auto entry_child_ino = value_out.get<ino_t>();
        std::string_view entry_name(reinterpret_cast<char*>(key_out.d_mdbval.mv_data),
                                    key_out.d_mdbval.mv_size);
        entry_name.remove_prefix(sizeof(ino_t));
        if (entry_name == name) {
            return make_result(entry_child_ino);
        }
    }

    return make_result(FAILED, -ENOENT);
}

Result<Stat> CacheTransactionRO::getattr(ino_t ino)
{
    MDBOutVal value{};
    if (ro_transaction()->get(db().inodes_db(), ino, value) == MDB_NOTFOUND) {
        return make_result(FAILED, -ENOENT);
    }

    auto parsed = Inode::parse(reinterpret_cast<std::uint8_t*>(value.d_mdbval.mv_data),
                               value.d_mdbval.mv_size);
    if (!parsed) {
        return copy_error(parsed);
    }

    return Stat{
        static_cast<InodeAttributes>(*parsed),
        .ino = ino,
    };
}

void CacheTransactionRO::abort()
{
    for (CommitHook &hook: m_commit_hooks) {
        if (hook.rollback) {
            hook.rollback();
        }
    }
    m_txn->abort();
    m_txn = nullptr;
}

Result<void> CacheTransactionRO::commit()
{
    if (!m_txn) {
        return make_result();
    }

    Result<void> result;
    auto iter = m_commit_hooks.begin();
    for (; iter != m_commit_hooks.end(); ++iter) {
        CommitHook &hook = *iter;
        if (!hook.stage_1_commit) {
            continue;
        }
        result = hook.stage_1_commit();
        if (!result) {
            // rollback!
            --iter;
            do {
                CommitHook &rb_hook = *iter;
                if (rb_hook.stage_1_rollback) {
                    rb_hook.stage_1_rollback();
                }
            } while (iter != m_commit_hooks.end());
            abort();
            return result;
        }
    }

    // now all preparations have passed, we can go on and execute the commit
    for (CommitHook &hook: m_commit_hooks) {
        if (hook.stage_2_commit) {
            hook.stage_2_commit();
        }
    }

    m_txn->commit();
    m_commit_hooks.clear();
    m_txn = nullptr;
    return make_result();
}

/* Dragonstash::CacheTransactionRW */

CacheTransactionRW::CacheTransactionRW(CacheDatabase &cache, MDBRWTransaction &&txn):
    CacheTransactionRO(cache, std::move(txn))
{

}

ino_t CacheTransactionRW::allocate_next_inode()
{
    MDBRWTransaction sub_txn = rw_transaction()->getRWTransaction();
    MDBOutVal value{};
    if (sub_txn->get(db().meta_db(), META_KEY_NEXT_INO, value) == MDB_NOTFOUND) {
        throw std::runtime_error("database corrupt: could not fetch next inode");
    }
    const auto result = value.get<ino_t>();
    const ino_t next = result + 1;
    sub_txn->put(db().meta_db(), META_KEY_NEXT_INO, next);
    sub_txn->commit();
    return result;
}

Result<ino_t> CacheTransactionRW::emplace(ino_t parent, std::string_view name,
                                          const InodeAttributes &attrs)
{
    {
        auto name_ok = db().check_name(name, true);
        if (!name_ok) {
            return make_result(FAILED, name_ok.error());
        }
    }

    Inode inode{
        attrs,
        .parent = parent
    };
    ino_t ino = allocate_next_inode();

    // Check if inode exists and unlink if needed
    // TODO: unlink recursively if inode is a directory
    // TOOD: remove data if non-directory
    // TODO: move to orphan list instead and allow cleanup in separate threads
    {
        auto name_cursor = rw_transaction()->getRWCursor(db().tree_name_key_db());
        std::string key;
        key.resize(sizeof(ino_t) + name.length());
        memcpy(&key[0], &parent, sizeof(ino_t));
        memcpy(&key[sizeof(ino_t)], name.data(), name.length());

        MDBOutVal key_out{};
        MDBOutVal value_out{};
        if (name_cursor.find(key, key_out, value_out) != MDB_NOTFOUND) {
            const auto old_ino = value_out.get<ino_t>();
            auto guard = db().in_memory_lock_guard();
            name_cursor.del();

            // re-use the key to construct the key for the other databases
            key.resize(sizeof(ino_t)*2);
            memcpy(&key[sizeof(ino_t)], &old_ino, sizeof(ino_t));
            rw_transaction()->del(db().tree_inode_key_db(), key);
            /* check if inode is locked via in-memory lock */
            if (db().in_memory_locks()[old_ino] <= 0) {
                rw_transaction()->del(db().inodes_db(), old_ino);
            } else {
                // we still have to set the parent to zero
                auto cursor = rw_transaction()->getCursor(db().inodes_db());
                if (cursor.find(old_ino, key_out, value_out) == 0) {
                    auto old_inode = Inode::parse(reinterpret_cast<std::uint8_t*>(value_out.d_mdbval.mv_data),
                                                  value_out.d_mdbval.mv_size);
                    if (old_inode) {
                        old_inode->parent = 0;
                        std::string value_buf;
                        value_buf.resize(Inode::serialized_size);
                        old_inode->serialize(reinterpret_cast<std::uint8_t*>(value_buf.data()));
                        cursor.put(key_out, value_buf);
                    }
                }
            }
        }
    }

    // write inode
    {
        MDBInVal key(ino);
        std::vector<std::uint8_t> buf;
        buf.resize(Inode::serialized_size);
        inode.serialize(buf.data());
        std::string_view value(reinterpret_cast<char*>(buf.data()), buf.size());
        rw_transaction()->put(db().inodes_db(), key, value);
    }

    // TODO: orphan and delete old inode
    // write directory entry pair
    {
        std::vector<std::uint8_t> buf;
        buf.resize(sizeof(ino_t) + name.length());
        memcpy(&buf[0], &parent, sizeof(ino_t));
        memcpy(&buf[sizeof(ino_t)], name.data(), name.length());
        std::string_view key(reinterpret_cast<char*>(buf.data()), buf.size());
        rw_transaction()->put(db().tree_name_key_db(), key, ino);
    }

    {
        std::array<char, sizeof(ino_t)*2> key_buf{};
        memcpy(&key_buf[0], &parent, sizeof(ino_t));
        memcpy(&key_buf[sizeof(ino_t)], &ino, sizeof(ino_t));
        std::string_view key(key_buf.data(), key_buf.size());
        rw_transaction()->put(db().tree_inode_key_db(), key, name);
    }

    return ino;
}

Result<void> CacheTransactionRW::lock(ino_t ino)
{
    {
        auto guard = db().in_memory_lock_guard();
        db().in_memory_locks()[ino] += 1;
    }

    add_commit_hook(nullptr, nullptr, nullptr, [this, ino](){
        auto guard = db().in_memory_lock_guard();
        safe_dec(db().in_memory_locks()[ino]);
    });
    return make_result();
}

Result<void> CacheTransactionRW::release(ino_t ino)
{
    bool cleared;
    {
        auto guard = db().in_memory_lock_guard();
        cleared = safe_dec(db().in_memory_locks()[ino]) == 0;
    }
    add_commit_hook(nullptr, nullptr, nullptr, [this, ino](){
        auto guard = db().in_memory_lock_guard();
        db().in_memory_locks()[ino] += 1;
    });

    if (!cleared) {
        return make_result();
    }

    // inode may have been orphaned, check for deletion
    auto inode_cursor = rw_transaction()->getRWCursor(db().inodes_db());
    MDBOutVal key_out{};
    MDBOutVal value_out{};
    if (inode_cursor.find(ino, key_out, value_out) == MDB_NOTFOUND) {
        // this does not automatically roll back the transaction though...
        return make_result(FAILED, -ENOENT);
    }

    auto inode = Inode::parse(reinterpret_cast<std::uint8_t*>(value_out.d_mdbval.mv_data),
                              value_out.d_mdbval.mv_size);
    if (!inode) {
        return copy_error(inode);
    }

    if (inode->parent == INVALID_INO) {
        // orphaned inode
        // TODO: clean up related data; since it's orphaned, it doesn't have
        // any directory entries anymore, but blocks and link data may still
        // exist.
        inode_cursor.del();
    }

    return make_result();
}

}
