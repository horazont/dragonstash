/**********************************************************************
File name: fs.hpp
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
#ifndef DRAGONSTASH_FS_H
#define DRAGONSTASH_FS_H

#include <atomic>
#include <memory>

#include "fuse/interface.hpp"
#include "backend.hpp"
#include "cache/cache.hpp"

namespace Dragonstash {

class Filesystem: public Fuse::Interface
{
public:
    Filesystem() = delete;
    explicit Filesystem(Cache &cache, Backend::Filesystem &backend);

private:
    Cache &m_cache;
    Backend::Filesystem &m_backend_fs;

    Result<std::string> get_backend_path(CacheTransactionRO &txn, ino_t ino);

public:
    void lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name);
    void forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup);
    void getattr(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readlink(Fuse::Request &&req, fuse_ino_t ino);
    void opendir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readdir(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void releasedir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void forget_multi(Fuse::Request &&req, size_t count, struct fuse_forget_data *forgets);
    /* void forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup); */

};

}

#endif
