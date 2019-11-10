#ifndef DRAGONSTASH_BACKEND_H
#define DRAGONSTASH_BACKEND_H

#include <cstdint>
#include <string>
#include <memory>
#include <optional>

#include "error.hpp"

namespace Dragonstash {
namespace Backend {

struct Stat {
    uint32_t mode;
    uint64_t size;
    uint64_t ino;
    uint32_t uid;
    uint32_t gid;
    struct timespec atime, mtime, ctime;
};


struct DirEntry: public Stat {
    std::string name;
    bool complete;
};


class File {
public:
    virtual ~File();

public:
    virtual Result<Stat> fstat() = 0;
    virtual Result<ssize_t> pread(void *buf, size_t count, off_t offset) = 0;
    virtual Result<ssize_t> pwrite(const void  *buf, size_t count, off_t offset) = 0;
    virtual Result<void> fsync() = 0;
    virtual Result<void> close() = 0;

};


class Dir {
public:
    virtual ~Dir();

public:
    virtual Result<DirEntry> readdir() = 0;
    virtual Result<void> fsyncdir() = 0;
    virtual Result<void> closedir() = 0;

};

class Filesystem {
public:
    virtual ~Filesystem();

public:
    virtual Result<std::unique_ptr<File>> open(std::string_view path,
                                               int accesstype,
                                               mode_t mode) = 0;
    virtual Result<std::unique_ptr<Dir>> opendir(std::string_view path) = 0;
    virtual Result<Stat> lstat(std::string_view path) = 0;
    virtual Result<std::string> readlink(std::string_view path) = 0;

};

}
}

#endif
