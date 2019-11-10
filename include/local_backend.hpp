/**********************************************************************
File name: local_backend.hpp
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

    Result<std::string> map_path(std::string_view s);

    // Filesystem interface
public:
    [[nodiscard]] Result<std::unique_ptr<File>> open(std::string_view path,
                                                     int accesstype,
                                                     mode_t mode) override;
    [[nodiscard]] Result<std::unique_ptr<Dir> > opendir(std::string_view path) override;
    [[nodiscard]] Result<Stat> lstat(std::string_view path) override;
    [[nodiscard]] Result<std::string> readlink(std::string_view path) override;

};

}
}

#endif
