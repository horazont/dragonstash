/**********************************************************************
File name: fs.cpp
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
#include "fs.hpp"

#include "fuse/buffer.hpp"

namespace Dragonstash {

Filesystem::Filesystem(Cache &cache, Backend::Filesystem &backend):
    m_cache(cache),
    m_backend_fs(backend)
{

}

void Filesystem::lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name)
{
    auto txn = m_cache.begin_rw();

    Result<ino_t> ino_result = make_result(FAILED, -EOPNOTSUPP);
    struct fuse_entry_param e{};
    e.attr_timeout = 1.0;
    e.entry_timeout = 1.0;

    auto path_result = txn.path(parent);
    if (!path_result) {
        req.reply_err(path_result.error());
        return;
    }

    std::string backend_path = *path_result;
    backend_path.reserve(backend_path.size() + name.size() + 1);
    backend_path += '/';
    backend_path += name;
    std::string_view backend_path_view(backend_path);

    auto stat_result = m_backend_fs.lstat(backend_path_view);
    if (!stat_result && stat_result.error() != ENOTCONN) {
        req.reply_err(stat_result.error());
        return;
    }

    if (stat_result) {
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
        // backend not connected, retrieve from cache if available
        ino_result = txn.lookup(parent, name);
        if (!ino_result) {
            if (ino_result.error() == ENOENT) {
                // XXX: This is a tricky one, because we currently do not know
                // whether the entry is currently not in the cache (e.g. because
                // the parent has never been opendir()'d) or whether the entry
                // truly does not exist on the remote.
                // we return ENOENT for now, but we might want to return either
                // ENOTCONN or EIO later on. Also, we might want negative
                // caching / a flag on a directory indicating that it has been
                // full-sync’d in opendir once.
            }
            req.reply_err(ino_result.error());
            return;
        }

        auto attr_result = txn.getattr(*ino_result);
        if (!attr_result) {
            // return EIO, because this is not supposed to even happen...
            req.reply_err(EIO);
            return;
        }

        e.attr = *attr_result;
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
    auto txn = m_cache.begin_ro();
    auto release_result = txn.release(ino, nlookup);
    if (!release_result) {
        req.reply_err(release_result.error());
        return;
    }
    (void)txn.commit();
    req.reply_none();
}

void Filesystem::getattr(Fuse::Request &&req, fuse_ino_t ino, fuse_file_info *fi)
{
    auto txn = m_cache.begin_ro();
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
    auto txn = m_cache.begin_rw();
    auto path_result = txn.path(ino);
    if (!path_result) {
        req.reply_err(path_result.error());
        return;
    }
    std::string_view backend_path;

    if (path_result->empty()) {
        backend_path = "/";
    } else {
        backend_path = *path_result;
    }

    auto dir = m_backend_fs.opendir(backend_path);
    if (!dir) {
        req.reply_err(dir.error());
        return;
    }

    while (auto entry = (*dir)->readdir()) {
        std::string entry_path(backend_path);
        entry_path.reserve(entry_path.size() + entry->name.size() + 1);
        if (entry_path.size() > 1) {
            // need to add a slash to the end
            entry_path += '/';
        }
        entry_path += entry->name;
        auto stat_result = m_backend_fs.lstat(entry_path);
        if (!stat_result) {
            continue;
        }
        const auto info = Inode::from_backend_stat(*stat_result);
        (void)txn.emplace(ino, entry->name, info);
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
    auto txn = m_cache.begin_ro();
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
    auto txn = m_cache.begin_ro();
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
    auto txn = m_cache.begin_ro();
    for (std::size_t i = 0; i < count; ++i) {
        (void)txn.release(forgets[i].ino, forgets[i].nlookup);
    }
    (void)txn.commit();
    req.reply_none();
}

}
