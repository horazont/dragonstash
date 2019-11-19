/**********************************************************************
File name: backend.hpp
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
#ifndef DRAGONSTASH_BACKEND_H
#define DRAGONSTASH_BACKEND_H

#include <cstdint>
#include <string>
#include <memory>
#include <optional>

#include "error.hpp"

namespace Dragonstash::Backend {

template <typename T>
static inline bool is_not_connected(const Result<T> &result) {
    return !result && result.error() == ENOTCONN;
}

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

#endif
