#ifndef DRAGONSTASH_LOCAL_BACKEND_H
#define DRAGONSTASH_LOCAL_BACKEND_H

#include <filesystem>

#include <dirent.h>

#include "backend.hpp"

namespace Dragonstash {
namespace Backend {

class LocalFile: public File {
public:
    LocalFile(int fd);
    ~LocalFile() override;

private:
    int m_fd;

    // File interface
public:
    Result<Stat> fstat() override;
    Result<ssize_t> pread(void *buf, size_t count, off_t offset) override;
    Result<ssize_t> pwrite(const void *buf, size_t count, off_t offset) override;
    Result<void> fsync() override;
    Result<void> close() override;
};

class LocalDir: public Dir {
public:
    explicit LocalDir(::DIR *fd);
    ~LocalDir() override;

private:
    ::DIR *m_fd;

    // Dir interface
public:
    Result<DirEntry> readdir() override;
    Result<void> fsyncdir() override;
    Result<void> closedir() override;
};

class LocalFilesystem: public Filesystem {
public:
    explicit LocalFilesystem(const std::filesystem::path &root);

private:
    std::filesystem::path m_root;

    Result<std::string> map_path(const std::string &s);

    // Filesystem interface
public:
    std::unique_ptr<File> open(const std::string &path,
                               int accesstype,
                               mode_t mode) override;
    std::unique_ptr<Dir> opendir(const std::string &path) override;
    Result<Stat> lstat(const std::string &path) override;

};

}
}

#endif
