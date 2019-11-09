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
    explicit Filesystem(std::unique_ptr<Cache> &&cache);

private:
    std::unique_ptr<Cache> m_cache;
    std::unique_ptr<Backend::Filesystem> m_backend_fs;

    Result<std::string> get_backend_path(CacheTransactionRO &txn, ino_t ino);

public:
    std::unique_ptr<Backend::Filesystem> reset_backend_fs(std::unique_ptr<Backend::Filesystem> &&backend);

public:
    void lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name);
    void forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup);
    void getattr(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void opendir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readdir(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void releasedir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void forget_multi(Fuse::Request &&req, size_t count, struct fuse_forget_data *forgets);
    /* void forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup); */

};

}

#endif
