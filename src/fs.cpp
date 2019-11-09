#include "fs.hpp"

#include "fuse/buffer.hpp"

namespace Dragonstash {

Filesystem::Filesystem(std::unique_ptr<Cache> &&cache):
    m_cache(std::move(cache))
{

}

std::unique_ptr<Backend::Filesystem> Filesystem::reset_backend_fs(std::unique_ptr<Backend::Filesystem> &&backend)
{
    std::unique_ptr<Backend::Filesystem> tmp = std::move(backend);
    std::swap(tmp, m_backend_fs);
    return tmp;
}

void Filesystem::lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name)
{
    auto txn = m_cache->begin_rw();

    Result<ino_t> ino_result = make_result(FAILED, -EOPNOTSUPP);
    struct fuse_entry_param e{};
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;

    if (m_backend_fs) {
        auto path_result = txn.path(parent);
        if (!path_result) {
            req.reply_err(path_result.error());
            return;
        }

        std::string backend_path = *path_result;
        backend_path.reserve(backend_path.size() + name.size() + 1);
        if (path_result->size() > 1) {
            backend_path += '/';
        }
        backend_path += name;
        std::string_view backend_path_view(backend_path);
        // remove leading slash because the backend FS won’t like it
        backend_path_view.remove_prefix(1);

        auto stat_result = m_backend_fs->lstat(backend_path_view);
        if (!stat_result) {
            req.reply_err(stat_result.error());
            return;
        }
        auto cache_attrs = Inode::from_backend_stat(*stat_result);

        ino_result = txn.emplace(parent, name, cache_attrs);
        if (!ino_result) {
            req.reply_err(ino_result.error());
            return;
        }

        e.attr = Stat{
            InodeAttributes(cache_attrs),
            .ino = *ino_result
        };
    } else {
        ino_result = txn.lookup(parent, name);

        auto stat_result = txn.getattr(*ino_result);
        if (!stat_result) {
            req.reply_err(stat_result.error());
            return;
        }

        e.attr = *stat_result;
    }

    auto lock_result = txn.lock(*ino_result);
    if (!lock_result) {
        req.reply_err(lock_result.error());
        return;
    }
    e.ino = *ino_result;

    auto commit_result = txn.commit();
    if (!commit_result) {
        req.reply_err(commit_result.error());
        return;
    }
    req.reply_entry(&e);
}

void Filesystem::forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup)
{
    auto txn = m_cache->begin_ro();
    auto release_result = txn.release(ino, nlookup);
    if (!release_result) {
        req.reply_err(release_result.error());
        return;
    }
    (void)txn.commit();
}

void Filesystem::getattr(Fuse::Request &&req, fuse_ino_t ino, fuse_file_info *fi)
{
    auto txn = m_cache->begin_ro();
    auto getattr_result = txn.getattr(ino);
    if (!getattr_result) {
        req.reply_err(getattr_result.error());
        return;
    }

    struct stat stbuf = *getattr_result;
    req.reply_attr(stbuf, 1.0);
}

void Filesystem::opendir(Fuse::Request &&req, fuse_ino_t ino, fuse_file_info *fi)
{
    auto txn = m_cache->begin_rw();
    auto path_result = txn.path(ino);
    if (!path_result) {
        req.reply_err(path_result.error());
        return;
    }

    std::string_view backend_path(*path_result);
    backend_path.remove_prefix(1);

    auto dir = m_backend_fs->opendir(backend_path);
    if (!dir) {
        req.reply_err(dir.error());
        return;
    }

    while (auto entry = (*dir)->readdir()) {
        (void)txn.emplace(ino, entry->name, Inode::from_backend_stat(*entry));
    }

    fi->fh = 0;
    fi->cache_readdir = 1;
    if (!txn.commit()) {
        req.reply_err(EIO);
        return;
    }

    req.reply_open(fi);
}

void Filesystem::readdir(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, fuse_file_info *fi)
{
    Fuse::DirBuffer buffer;
    int error = 0;
    off_t cursor = off;
    std::size_t to_send = 0;
    auto txn = m_cache->begin_ro();
    while (buffer.length() < size) {
        to_send = buffer.length();

        auto readdir_result = txn.readdir(ino, cursor);
        if (!readdir_result) {
            error = readdir_result.error();
            break;
        }

        struct stat buf{};
        if (!readdir_result->complete) {
            auto stat_result = txn.getattr(readdir_result->ino);
            if (!stat_result) {
                error = stat_result.error();
                break;
            }
            buf = *stat_result;
        } else {
            buf = *readdir_result;
        }

        cursor = readdir_result->ino;
        buffer.add(req, readdir_result->name.c_str(), buf, readdir_result->ino);
    }

    if (error != 0) {
        req.reply_err(error);
    }

    auto buf = buffer.get();
    req.reply_buf(buf.data(), to_send);
}

void Filesystem::releasedir(Fuse::Request &&req, fuse_ino_t ino, fuse_file_info *fi)
{
    req.reply_none();
}

void Filesystem::readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, fuse_file_info *fi)
{
    Fuse::DirBufferPlus buffer;
    int error = 0;
    off_t cursor = off;
    std::size_t to_send = 0;
    auto txn = m_cache->begin_ro();
    while (buffer.length() < size) {
        to_send = buffer.length();

        auto readdir_result = txn.readdir(ino, cursor);
        if (!readdir_result) {
            error = readdir_result.error();
            break;
        }

        struct fuse_entry_param e{};
        e.ino = readdir_result->ino;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;

        if (!readdir_result->complete) {
            auto stat_result = txn.getattr(readdir_result->ino);
            if (!stat_result) {
                error = stat_result.error();
                break;
            }
            e.attr = *stat_result;
        } else {
            e.attr = *readdir_result;
        }

        buffer.add(req, readdir_result->name.c_str(), e, readdir_result->ino);
        cursor = readdir_result->ino;

        if (readdir_result->name != "." && readdir_result->name != "..") {
            auto lock_result = txn.lock(readdir_result->ino);
            if (!lock_result) {
                if (lock_result.error() == ESTALE) {
                    // inode has been deleted in the future -> skip this entry
                    buffer.rewind(to_send);
                    continue;
                }
                req.reply_err(EIO);
                return;
            }
        }
    }

    if (error != 0) {
        req.reply_err(error);
        return;
    }

    auto buf = buffer.get();
    if (!txn.commit()) {
        // if the commit fails, we cannot hand out any locks -> we have to
        // return an error ... Question is if we may want to return ENOSYS
        // instead which would make things possibly fall back to readdir.
        req.reply_err(EIO);
        return;
    }

    req.reply_buf(buf.data(), to_send);
}

void Filesystem::forget_multi(Fuse::Request &&req, size_t count, fuse_forget_data *forgets)
{
    auto txn = m_cache->begin_ro();
    for (std::size_t i = 0; i < count; ++i) {
        (void)txn.release(forgets[i].ino, forgets[i].nlookup);
    }
    (void)txn.commit();
    req.reply_none();
}

}
