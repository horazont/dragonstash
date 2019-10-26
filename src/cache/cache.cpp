#include "cache/cache.hpp"

#include <cassert>
#include <sys/stat.h>

/**
 * LMDB database layout
 *
 * Database `inodes` (MDB_INTEGERKEY):
 *
 * - key: uint64_t inode
 * - value: uint8_t version + struct InodeV1 + type-specific inode data
 *
 * Database `dirs` (MDB_DUPSORT):
 *
 * - key: uint64_t inode
 * - value: uint8_t version + uint64_t inode + uint32_t mode cache + name
 */

namespace Dragonstash {

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

Result<DirEntry> CachedDir::parse_entry(const char *buf, size_t sz)
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

Result<DirEntry> CachedDir::parse_entry_v1(const char *buf, size_t sz)
{
    if (sz <= sizeof(std::uint64_t) + sizeof(uint32_t)) {
        return make_result(FAILED, -EIO);
    }

    DirEntry result;
    memcpy(&result.ino, &buf[0], sizeof(result.ino));
    buf += sizeof(result.ino); sz -= sizeof(result.ino);

    memcpy(&result.mode, &buf[0], sizeof(result.mode));
    buf += sizeof(result.mode); sz -= sizeof(result.mode);

    assert(sz >= 1);
    result.name = std::string(&buf[0], sz);

    return result;
}

Result<std::tuple<DirEntry, off_t> > CachedDir::ReadDir()
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


    Result<DirEntry> result =
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

Cache::Cache(const std::filesystem::path &db_path):
    m_env(getMDBEnv((db_path / "db").c_str(), MDB_NOSUBDIR, 0700)),
    m_inodes_db(m_env->openDB("inodes", MDB_CREATE)),
    m_tree_db(m_env->openDB("tree", MDB_DUPSORT | MDB_CREATE))
{

}

Result<ino_t> Cache::lookup(MDBRWTransaction &transaction, ino_t parent, const char *name)
{
    Result<std::unique_ptr<CachedDir>> dir = openDir(transaction, parent);
    if (!dir) {
        return make_result(FAILED, dir.error());
    }
    assert(*dir);
    Result<DirEntry> entry(FAILED, 0);
    while ((entry = (*dir)->ReadDir())) {
        if (entry->name == name) {
            return entry->ino;
        }
    }
}

Result<ino_t> Cache::PutAttr(ino_t parent, const char *name,
                             const Backend::Stat &data)
{
    Inode inode_data = Inode::from_backend_stat(data);
    auto txn = m_env->getRWTransaction();
}

Result<ino_t> Cache::Lookup(ino_t parent, const char *path)
{
    return make_result(FAILED, -ENOENT);
}

Result<Inode> Cache::GetAttr(ino_t ino)
{
    if (ino != ROOT_INO) {
        return make_result(FAILED, -ENOENT);
    }

    return Inode{
        .mode = S_IFDIR,
    };
}

Result<std::unique_ptr<CachedDir> > Cache::OpenDir(ino_t ino)
{
    if (ino != ROOT_INO) {
        return make_result(FAILED, -ENOENT);
    }

    auto transaction = m_env->getROTransaction();
    auto cursor = transaction.getCursor(m_tree_db);
    return std::make_unique<CachedDir>(std::move(transaction),
                                       std::move(cursor),
                                       ino);
}

}
