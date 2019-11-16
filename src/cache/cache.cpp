/**********************************************************************
File name: cache.cpp
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
#include "cache/cache.hpp"

#include <cassert>
#include <sys/stat.h>
#include <unistd.h>
#include <ctime>

#include "cache/direntry.hpp"

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


template <typename T>
constexpr void safe_assert(T &&x) {
    assert(x);
}

namespace Dragonstash {

static const std::string_view DB_NAME_META = "meta";
static const std::string_view DB_NAME_INODES = "inodes";
static const std::string_view DB_NAME_TREE_INODE_KEY = "treei";
static const std::string_view DB_NAME_TREE_NAME_KEY = "treen";
static const std::string_view DB_NAME_ORPHANS = "orphans";
static const std::string_view DB_NAME_LINKS = "links";

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


static inline std::basic_string_view<std::byte> view(const MDBOutVal &val)
{
    return std::basic_string_view<std::byte>(
                reinterpret_cast<std::byte*>(val.d_mdbval.mv_data),
                val.d_mdbval.mv_size);
}


static inline Result<Inode> inode_from_lmdb(const MDBOutVal &val)
{
    return Inode::parse(view(val));
}


static inline Result<copyfree_wrap<Inode>> inode_from_lmdb_inplace(const MDBOutVal &val)
{
    return Inode::parse_inplace(view(val));
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
        return make_result(FAILED, EIO);
    }

    std::uint8_t version;
    memcpy(&version, &buf[0], sizeof(version));

    switch (version) {
    case 1:
        return parse_entry_v1(&buf[1], sz-1);
    default:
        return make_result(FAILED, EIO);
    }
}

Result<DirectoryEntry> CachedDir::parse_entry_v1(const char *buf, size_t sz)
{
    if (sz <= sizeof(std::uint64_t) + sizeof(uint32_t)) {
        return make_result(FAILED, EIO);
    }

    DirectoryEntry result{};
    result.complete = false;
    memcpy(&result.ino, &buf[0], sizeof(result.ino));
    buf += sizeof(result.ino); sz -= sizeof(result.ino);

    memcpy(&result.attr.mode, &buf[0], sizeof(result.attr.mode));
    buf += sizeof(result.attr.mode); sz -= sizeof(result.attr.mode);

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
        return make_result(FAILED, EINVAL);
    }
    if (offset > m_curr_off) {
        /* seeking forward is not supported for now; we should not need this
         * since directories are supposed to be a stream */
        return make_result(FAILED, EINVAL);
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
    m_orphan_db(m_env->openDB(DB_NAME_ORPHANS, MDB_CREATE)),
    m_links_db(m_env->openDB(DB_NAME_LINKS, MDB_CREATE)),
    m_max_name_length(0)
{
    validate_max_key_size();
}

void CacheDatabase::validate_max_key_size()
{
    const size_t max_key_size = mdb_env_get_maxkeysize(*m_env);
    if (max_key_size < sizeof(ino_t) * 2) {
        throw std::runtime_error("cannot use this version of LMDB. maxkeysize too small.");
    }
    m_max_name_length = max_key_size - sizeof(ino_t);
}

Result<void> CacheDatabase::check_name(std::string_view name, bool for_writing)
{
    if (name.length() > m_max_name_length) {
        return make_result(FAILED, ENAMETOOLONG);
    }
    if (!for_writing) {
        return make_result();
    }

    // more thorough checks before persisting a name into the db
    for (char ch: name) {
        switch (ch) {
        case 0:
        case '/':
            return make_result(FAILED, EINVAL);
        default:;
        }
    }
    return make_result();
}


/* Dragonstash::Cache */

template<typename T>
auto with_rw_txn(CacheTransactionRW &&txn, T &&f) -> decltype(f(txn))
{
    auto result = f(txn);
    if (!result) {
        txn.abort();
        return result;
    }
    (void)txn.clean_orphans();
    {
        auto commit_result = txn.commit();
        if (!commit_result) {
            return make_result(FAILED, commit_result.error());
        }
    }
    return result;
}

const bool Cache::deadlock_detection = debug_mutex::is_safe;

Cache::Cache(const std::filesystem::path &db_path):
    m_db(getMDBEnv((db_path / "db").c_str(), MDB_NOSUBDIR, 0600))
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
        Inode root = mkinode(
                    InodeAttributes{
                        .common = CommonFileAttributes{
                            .uid = getuid(),
                            .gid = getgid(),
                            .atime = now,
                            .mtime = now,
                            .ctime = now,
                        },
                        .mode = S_IFDIR,
                    }
        );
        const ino_t root_ino = ROOT_INO;
        auto buf = serialize_as<char>(root);
        txn->put(m_db.inodes_db(), root_ino, buf);
    }
    txn->commit();

    with_rw_txn(begin_rw(), [](CacheTransactionRW &txn){
        return txn.clean_orphans();
    });
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

Result<std::string> Cache::readlink(ino_t ino)
{
    return begin_ro().readlink(ino);
}

Result<void> Cache::writelink(ino_t ino, std::string_view dest)
{
    return with_rw_txn(begin_rw(), [ino, &dest](CacheTransactionRW &txn){
        return txn.writelink(ino, dest);
    });
}

Result<std::string> Cache::path(ino_t ino)
{
    return begin_ro().path(ino);
}

/* Dragonstash::CacheTransactionRO */

CacheTransactionRO::CacheTransactionRO(CacheDatabase &db, MDBROTransaction &&txn,
                                       CacheTransactionRW *parent):
    m_db(&db),
    m_txn(std::move(txn)),
    m_parent(parent)
{

}

InodeReferences &CacheTransactionRO::inode_in_memory_locks() {
    if (m_inode_counter_lock) {
        return db().in_memory_locks();
    }

    CacheTransactionRW *parent = m_parent;
    while (parent) {
        if (parent->m_inode_counter_lock) {
            return parent->db().in_memory_locks();
        }
        parent = parent->m_parent;
    }

    // we don’t hold the lock and no parent holds the lock -> we need to lock
    // here.
    assert(!m_inode_counter_lock);
    m_inode_counter_lock = db().in_memory_lock_guard();
    return db().in_memory_locks();
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
        (void)rc;
        return make_result(FAILED, ENOENT);
    }

    auto parse_result = DirEntry::parse(view(value));
    if (!parse_result) {
        return copy_error(parse_result);
    }
    return make_result(std::get<1>(*parse_result));
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
    if (ino == ROOT_INO) {
        return ino;
    }

    MDBOutVal value{};
    if (int rc = ro_transaction()->get(db().inodes_db(), ino, value) == MDB_NOTFOUND) {
        (void)rc;
        return make_result(FAILED, ENOENT);
    }

    auto parsed = inode_from_lmdb(value);
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
        return make_result(FAILED, EINVAL);
    }

    auto cursor = ro_transaction()->getROCursor(m_db->tree_name_key_db());

    MDBOutVal key_out{};
    MDBOutVal value_out{};

    for (int rc = cursor.lower_bound(parent, key_out, value_out);
         rc != MDB_NOTFOUND;
         rc = cursor.nextprev(key_out, value_out, MDB_NEXT))
    {
        assert(key_out.d_mdbval.mv_size >= sizeof(ino_t));
        auto parse_result = DirEntry::parse_inplace(view(value_out));
        assert(parse_result);

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

        auto entry_child_ino = std::get<0>(*parse_result)->entry_ino;
        std::string_view entry_name(reinterpret_cast<char*>(key_out.d_mdbval.mv_data),
                                    key_out.d_mdbval.mv_size);
        entry_name.remove_prefix(sizeof(ino_t));
        if (entry_name == name) {
            return make_result(entry_child_ino);
        }
    }

    return make_result(FAILED, ENOENT);
}

Result<Stat> CacheTransactionRO::getattr(ino_t ino)
{
    MDBOutVal value{};
    if (ro_transaction()->get(db().inodes_db(), ino, value) == MDB_NOTFOUND) {
        return make_result(FAILED, ENOENT);
    }

    auto parsed = inode_from_lmdb(value);
    if (!parsed) {
        return copy_error(parsed);
    }

    return Stat{
        InodeAttributes(parsed->attr),
        ino,
    };
}

Result<std::string> CacheTransactionRO::readlink(ino_t ino)
{
    MDBOutVal value{};
    if (ro_transaction()->get(db().links_db(), ino, value) == MDB_NOTFOUND) {
        // check if the entry exists at all; if so, it is not a link -> EINVAL
        // otherwise, it does not exist -> ENOENT
        if (ro_transaction()->get(db().inodes_db(), ino, value) == MDB_NOTFOUND) {
            return make_result(FAILED, ENOENT);
        }
        return make_result(FAILED, EINVAL);
    }

    return std::string(reinterpret_cast<char*>(value.d_mdbval.mv_data),
                       value.d_mdbval.mv_size);
}

Result<DirectoryEntry> CacheTransactionRO::readdir(ino_t dir, ino_t prev_end)
{
    if (prev_end == 0) {
        // return dot
        return make_result(DirectoryEntry{
                               Stat{
                                   .ino = dir,
                               },
                               std::string("."),
                               false,
                           });
    }
    auto parent_result = parent(dir);
    if (!parent_result) {
        return make_result(FAILED, EIO);
    }
    if (prev_end != *parent_result && prev_end == dir) {
        // dot dot
        return make_result(DirectoryEntry{
                               Stat{
                                   .ino = *parent_result,
                               },
                               std::string(".."),
                               false,
                           });
    }

    auto cursor = ro_transaction()->getCursor(db().tree_inode_key_db());
    std::array<ino_t, 2> key{{dir, prev_end}};
    if (prev_end == *parent_result) {
        // start iteration from zero
        key[1] = 0;
    }
    const std::size_t key_bytes = key.size() * sizeof(decltype(key)::value_type);
    MDBInVal key_in(std::string_view(reinterpret_cast<char*>(key.data()),
                                     key_bytes));
    MDBOutVal key_out{};
    MDBOutVal value_out{};
    if (cursor.lower_bound(key_in, key_out, value_out) == MDB_NOTFOUND) {
        // EOF!
        return make_result(FAILED, 0);
    }
    assert(key_out.d_mdbval.mv_size == key_bytes);
    memcpy(key.data(), key_out.d_mdbval.mv_data, key_bytes);

    if (key[0] != dir) {
        // EOF!
        return make_result(FAILED, 0);
    }

    if (prev_end != 0 && key[1] == prev_end) {
        // advance by one for the next read
        if (cursor.next(key_out, value_out) == MDB_NOTFOUND) {
            // EOF!
            return make_result(FAILED, 0);
        }
        assert(key_out.d_mdbval.mv_size == key_bytes);
        memcpy(key.data(), key_out.d_mdbval.mv_data, key_bytes);

        if (key[0] != dir) {
            // EOF!
            return make_result(FAILED, 0);
        }
    }

    auto parse_result = DirEntry::parse(view(value_out));
    if (!parse_result) {
        return make_result(FAILED, EIO);
    }
    return make_result(DirectoryEntry{
                           Stat{
                               .ino = key[1],
                           },
                           std::string(std::get<1>(*parse_result)),
                           false,
                       });
}

Result<std::string> CacheTransactionRO::path(ino_t ino)
{
    if (ino == Dragonstash::ROOT_INO) {
        return "";
    }

    std::string buf;
    do {
        auto parent_result = parent(ino);
        if (!parent_result) {
            return copy_error(parent_result);
        }

        auto name_result = name(*parent_result, ino);
        if (!name_result) {
            return copy_error(name_result);
        }

        std::size_t offset = buf.size();
        buf.resize(offset + name_result->size() + 1);
        std::copy(name_result->rbegin(), name_result->rend(),
                  &buf[offset]);
        offset += name_result->size();
        buf[offset++] = '/';
        ino = *parent_result;
    } while (ino != ROOT_INO);

    std::reverse(buf.begin(), buf.end());
    return buf;
}

Result<void> CacheTransactionRO::lock(ino_t ino)
{
    auto &inode_locks = inode_in_memory_locks();
    auto inc_result = inode_locks.incref(ino, 1);
    if (!inc_result) {
        return copy_error(inc_result);
    }

    add_transaction_hook(nullptr, nullptr, nullptr, [&inode_locks, ino](){
        safe_assert(inode_locks.decref(ino, 1));
    });

    return make_result();
}

Result<void> CacheTransactionRO::release(ino_t ino, uint64_t nlocks)
{
    if (nlocks == 0) {
        return make_result();
    }

    auto &inode_locks = inode_in_memory_locks();
    safe_assert(inode_locks.decref(ino, nlocks));

    add_transaction_hook(nullptr, nullptr, nullptr, [&inode_locks, ino, nlocks](){
        safe_assert(inode_locks.incref(ino, nlocks));
    });

    return make_result();
}

Result<bool> CacheTransactionRO::test_flag(ino_t ino, InodeFlag flag)
{
    auto cursor = ro_transaction()->getCursor(db().inodes_db());
    const MDBInVal key_in(ino);
    MDBOutVal key_out{};
    MDBOutVal value_out{};
    if (cursor.find(key_in, key_out, value_out) != 0) {
        return make_result(FAILED, ENOENT);
    }
    auto inode = Inode::parse_inplace(std::basic_string_view<std::byte>(
                                          reinterpret_cast<std::byte*>(value_out.d_mdbval.mv_data),
                                          value_out.d_mdbval.mv_size
                                          ));
    if (!inode) {
        return copy_error(inode);
    }
    return (*inode)->test_flag(flag);
}

void CacheTransactionRO::abort()
{
    // two separate loops here to ensure that all stage 1 rollback callbacks
    // run before all main
    for (auto iter = m_transaction_hooks.rbegin();
         iter != m_transaction_hooks.rend();
         ++iter)
    {
        iter->stage_1_rollback();
    }
    for (auto iter = m_transaction_hooks.rbegin();
         iter != m_transaction_hooks.rend();
         ++iter)
    {
        iter->rollback();
    }
    m_txn->abort();
    m_txn = nullptr;
    if (m_inode_counter_lock) {
        m_inode_counter_lock.unlock();
    }
}

Result<void> CacheTransactionRO::commit()
{
    if (!m_txn) {
        return make_result();
    }

    if (!m_parent) {
        Result<void> result;
        auto iter = m_transaction_hooks.begin();
        for (; iter != m_transaction_hooks.end(); ++iter) {
            result = iter->stage_1_commit();
            if (!result) {
                abort();
                return result;
            }
        }

        // now all preparations have passed, we can go on and execute the commit
        for (TransactionHook &hook: m_transaction_hooks) {
            hook.stage_2_commit();
        }
    }

    m_txn->commit();
    if (m_parent) {
        // move all transaction hooks to parent: note that we don't execute any
        // of them in this transaction, not even the pre-checks.
        std::move(m_transaction_hooks.begin(),
                  m_transaction_hooks.end(),
                  std::back_inserter(m_parent->m_transaction_hooks));
    }
    m_transaction_hooks.clear();
    m_txn = nullptr;
    if (m_inode_counter_lock) {
        if (m_parent) {
            m_parent->m_inode_counter_lock = std::move(m_inode_counter_lock);
        } else {
            m_inode_counter_lock.unlock();
        }
    }
    return make_result();
}

/* Dragonstash::CacheTransactionRW */

CacheTransactionRW::CacheTransactionRW(CacheDatabase &cache, MDBRWTransaction &&txn, CacheTransactionRW *parent):
    CacheTransactionRO(cache, std::move(txn), parent)
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

Result<void> CacheTransactionRW::make_orphan(ino_t ino)
{
    Result<ino_t> parent = this->parent(ino);
    if (!parent) {
        return copy_error(parent);
    }
    if (*parent == INVALID_INO) {
        // already orphaned
        // TODO: double-check by looking in the orphan list?
        return make_result();
    }

    // when orphaning, we need to unlink the inode from the directory structure
    // and add it to the orphan database
    // we don't yet do any clean up or deletion.

    std::string key;
    key.resize(sizeof(ino_t)*2);
    memcpy(&key[0], &parent, sizeof(ino_t));
    memcpy(&key[sizeof(ino_t)], &ino, sizeof(ino_t));

    // look up inode name, calculate the lookup key for the name-keyed database
    // and delete the entry from the inode-keyed database.
    {
        MDBOutVal key_out{};
        MDBOutVal value_out{};
        auto ino_cursor = rw_transaction()->getRWCursor(db().tree_inode_key_db());
        if (ino_cursor.find(key, key_out, value_out) == MDB_NOTFOUND) {
            return make_result(FAILED, EIO);
        }
        auto direntry_result = DirEntry::parse(view(value_out));
        assert(direntry_result);
        std::string_view name = std::get<1>(*direntry_result);
        key.resize(sizeof(ino_t) + name.length());
        memcpy(&key[sizeof(ino_t)], name.data(), name.length());
        ino_cursor.del();
    }

    // delete the entry from the name-keyed database
    rw_transaction()->del(db().tree_name_key_db(), key);

    const std::uint8_t value = 0;
    // add the inode to the orphans
    rw_transaction()->put(db().orphan_db(), ino, value);

    // update the inode itself to set the parent to the invalid inode
    {
        MDBOutVal key_out{};
        MDBOutVal value_out{};
        auto cursor = rw_transaction()->getCursor(db().inodes_db());
        if (cursor.find(ino, key_out, value_out) == 0) {
            auto old_inode = inode_from_lmdb(value_out);
            if (old_inode) {
                old_inode->parent = 0;
                const auto value_buf = serialize_as<char>(*old_inode);
                cursor.put(key_out, value_buf);
            }
        }
    }

    return make_result();
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

    Inode inode = mkinode(attrs, parent);

    // orphan old inode if this emplace operation overwrites an existing inode
    {
        auto name_cursor = rw_transaction()->getRWCursor(db().tree_name_key_db());
        std::string key;
        key.resize(sizeof(ino_t) + name.length());
        memcpy(&key[0], &parent, sizeof(ino_t));
        memcpy(&key[sizeof(ino_t)], name.data(), name.length());

        MDBOutVal key_out{};
        MDBOutVal value_out{};
        if (name_cursor.find(key, key_out, value_out) != MDB_NOTFOUND) {
            ino_t old_ino;
            {
                auto parse_result = DirEntry::parse_inplace(view(value_out));
                assert(parse_result);
                old_ino = std::get<0>(*parse_result)->entry_ino;
            }
            // now we have to check whether the format differs
            auto ino_cursor = rw_transaction()->getRWCursor(db().inodes_db());
            assert(ino_cursor.find(old_ino, key_out, value_out) == 0);
            auto old_inode = inode_from_lmdb_inplace(value_out);
            assert(old_inode);
            if (((*old_inode)->attr.mode & S_IFMT) == (attrs.mode & S_IFMT)) {
                // do *not* remove, only update in-place
                const auto buf = serialize_as<char>(inode);
                ino_cursor.put(key_out, buf);
                return make_result(old_ino);
            }

            // if orphaning won't work, we can't be saved anyways
            (void)make_orphan(old_ino);
        }
    }

    ino_t ino = allocate_next_inode();

    // write inode
    {
        MDBInVal key(ino);
        const auto buf = serialize_as<char>(inode);
        rw_transaction()->put(db().inodes_db(), key, buf);
    }

    std::basic_string<std::byte> direntry_buffer;
    {
        DirEntry *entry;
        char *name_ptr;
        std::tie(entry, name_ptr) = Dragonstash::emplace(direntry_buffer, name.length(), ino);
        memcpy(name_ptr, name.data(), name.length());
    }
    std::string_view direntry_view(reinterpret_cast<char*>(direntry_buffer.data()),
                                   direntry_buffer.size());

    // write directory entry pair
    std::basic_string<std::byte> key_buf;
    {
        key_buf.resize(sizeof(ino_t) + name.length());
        memcpy(&key_buf[0], &parent, sizeof(ino_t));
        memcpy(&key_buf[sizeof(ino_t)], name.data(), name.length());
        std::string_view key(reinterpret_cast<char*>(key_buf.data()), key_buf.size());
        rw_transaction()->put(db().tree_name_key_db(), key, direntry_view);
    }

    {
        key_buf.resize(sizeof(ino_t)*2);
        memcpy(&key_buf[0], &parent, sizeof(ino_t));
        memcpy(&key_buf[sizeof(ino_t)], &ino, sizeof(ino_t));
        std::string_view key(reinterpret_cast<char*>(key_buf.data()), key_buf.size());
        rw_transaction()->put(db().tree_inode_key_db(), key, direntry_view);
    }

    (void)clean_orphans();

    return ino;
}

Result<void> CacheTransactionRW::clean_orphans()
{
    auto &inode_locks = inode_in_memory_locks();
    auto cursor = rw_transaction()->getRWCursor(db().orphan_db());
    MDBOutVal key_out{};
    MDBOutVal value_out{};
    int rc = cursor.nextprev(key_out, value_out, MDB_FIRST);
    while (rc == 0)
    {
        const auto ino = key_out.get<ino_t>();
        const auto doom_result = inode_locks.doom(ino);
        if (!doom_result) {
            // cannot doom -> need to skip
            rc = cursor.nextprev(key_out, value_out, MDB_NEXT);
            continue;
        }


        // TODO: clean up data associated with the inode:
        // - DONE: for S_IFDIR: orphan child inodes recursively
        // - for S_IFREG: delete cached blocks, release quota
        // - DONE: for S_IFLNK: delete link destination entry
        {
            auto inode_cursor = rw_transaction()->getCursor(db().inodes_db());
            MDBOutVal key_out{};
            MDBOutVal value_out{};
            if (inode_cursor.find(ino, key_out, value_out) == 0) {
                auto inode_res = inode_from_lmdb(value_out);
                if (inode_res) {
                    switch (inode_res->attr.mode & S_IFMT)
                    {
                    case S_IFLNK:
                    {
                        rw_transaction()->del(db().links_db(), ino);
                        break;
                    }
                    case S_IFDIR:
                    {
                        // orphan all child entries of this directory
                        auto dir_cursor = rw_transaction()->getCursor(db().tree_inode_key_db());
                        while (dir_cursor.lower_bound(ino, key_out, value_out) == 0) {
                            ino_t found_parent_ino;
                            memcpy(&found_parent_ino, key_out.d_mdbval.mv_data, sizeof(ino_t));
                            if (found_parent_ino != ino) {
                                // no more entries in this directory!
                                break;
                            }

                            auto direntry_result = DirEntry::parse(view(value_out));
                            assert(direntry_result);
                            const ino_t found_child_ino = std::get<0>(*direntry_result).entry_ino;
                            (void)make_orphan(found_child_ino);
                        }
                    }
                    default:;
                    }
                }
                inode_cursor.del();
            }
        }
        cursor.del();
        // XXX: this is very inefficient, but there doesn’t seem to be a safe
        // way to detect when the last item has been deleted...?
        rc = cursor.nextprev(key_out, value_out, MDB_FIRST);
    }
    return make_result();
}

Result<void> CacheTransactionRW::writelink(ino_t ino, std::string_view dest)
{
    MDBOutVal value_out{};
    if (rw_transaction()->get(db().links_db(), ino, value_out) == MDB_NOTFOUND) {
        // check inode type first
        auto stat_result = getattr(ino);
        if (!stat_result) {
            return copy_error(stat_result);
        }
        if ((stat_result->attr.mode & S_IFMT) != S_IFLNK) {
            // not a symlink -> EINVAL
            return make_result(FAILED, EINVAL);
        }
    }
    rw_transaction()->put(db().links_db(), ino, dest);
    return make_result();
}

Result<void> CacheTransactionRW::update_flags(ino_t ino, std::initializer_list<InodeFlag> to_set, std::initializer_list<InodeFlag> to_clear)
{
    auto cursor = rw_transaction()->getRWCursor(db().inodes_db());
    const MDBInVal key_in(ino);
    MDBOutVal key_out{};
    MDBOutVal value_out{};

    if (cursor.find(key_in, key_out, value_out) != 0) {
        return make_result(FAILED, ENOENT);
    }

    auto inode = inode_from_lmdb(value_out);
    if (!inode) {
        return copy_error(inode);
    }

    for (InodeFlag flag: to_set) {
        inode->set_flag(flag, true);
    }
    for (InodeFlag flag: to_clear) {
        inode->set_flag(flag, false);
    }

    const auto buf = serialize_as<char>(*inode);
    cursor.put(key_out, buf);

    return make_result();
}

}
