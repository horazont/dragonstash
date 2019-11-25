/**********************************************************************
File name: local_backend.cpp
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
#include "dragonstash/local_backend.hpp"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstring>

namespace Dragonstash {
namespace Backend {

inline Stat from_os_stat(struct stat &src)
{
    return Stat{
        .mode = src.st_mode,
        .size = static_cast<uint64_t>(src.st_size),
        .ino = src.st_ino,
        .uid = src.st_uid,
        .gid = src.st_gid,
        .atime = src.st_atim,
        .mtime = src.st_mtim,
        .ctime = src.st_ctim,
    };
}

LocalFile::LocalFile(int fd):
    m_fd(fd)
{

}

LocalFile::~LocalFile()
{
    if (m_fd >= 0) {
        close();
    }
}

Result<Stat> LocalFile::fstat()
{
    struct stat buf;
    memset(&buf, 0, sizeof(buf));
    if (::fstat(m_fd, &buf) < 0) {
        return Result<Stat>(FAILED, errno);
    }

    return from_os_stat(buf);
}

Result<ssize_t> LocalFile::pread(void *buf, size_t count, off_t offset)
{
    const ssize_t result = ::pread(m_fd, buf, count, offset);
    if (result < 0) {
        return Result<ssize_t>(FAILED, errno);
    }
    return result;
}

Result<ssize_t> LocalFile::pwrite(const void *buf, size_t count, off_t offset)
{
    const ssize_t result = ::pwrite(m_fd, buf, count, offset);
    if (result < 0) {
        return Result<ssize_t>(FAILED, errno);
    }
    return result;
}

Result<void> LocalFile::fsync()
{
    if (::fsync(m_fd) < 0) {
        return Result<void>(FAILED, errno);
    }
    return Result<void>();
}

Result<void> LocalFile::close()
{
    if (::close(m_fd) < 0) {
        return Result<void>(FAILED, errno);
    }
    m_fd = 0;
    return Result<void>();
}

LocalDir::LocalDir(DIR *fd):
    m_fd(fd)
{

}

LocalDir::~LocalDir()
{
    if (m_fd) {
        closedir();
    }
}

Result<DirEntry> LocalDir::readdir()
{
    struct dirent *entry = ::readdir(m_fd);
    if (!entry) {
        return Result<DirEntry>(FAILED, 0);
    }

    uint32_t st_mode = 0;
    switch (entry->d_type) {
    case DT_BLK:
        st_mode = S_IFBLK;
        break;
    case DT_CHR:
        st_mode = S_IFCHR;
        break;
    case DT_REG:
        st_mode = S_IFREG;
        break;
    case DT_DIR:
        st_mode = S_IFDIR;
        break;
    case DT_FIFO:
        st_mode = S_IFIFO;
        break;
    case DT_LNK:
        st_mode = S_IFLNK;
        break;
    case DT_SOCK:
        st_mode = S_IFSOCK;
        break;
    }

    return DirEntry{
        Stat{
            .mode = st_mode,
            .ino = entry->d_ino,
        },
        entry->d_name,
        false,
    };
}

Result<void> LocalDir::fsyncdir()
{
    if (::fsync(dirfd(m_fd)) < 0) {
        return Result<void>(FAILED, errno);
    }
    return Result<void>();
}

Result<void> LocalDir::closedir()
{
    if (::closedir(m_fd) < 0) {
        return Result<void>(FAILED, errno);
    }
    m_fd = 0;
    return Result<void>();
}

LocalFilesystem::LocalFilesystem(const std::filesystem::path &root):
    m_root(root)
{

}

Result<std::string> LocalFilesystem::map_path(std::string_view s)
{
    if (s.empty()) {
        return make_result(FAILED, EINVAL);
    }
    if (s[0] != '/') {
        return make_result(FAILED, EINVAL);
    }
    s.remove_prefix(1);
    std::filesystem::path inner(s);
    if (!inner.is_relative()) {
        return make_result(FAILED, EINVAL);
    }

    std::filesystem::path full_path(m_root);
    full_path /= inner;
    return full_path.native();
}

Result<std::unique_ptr<File> > LocalFilesystem::open(std::string_view path,
                                                     int accesstype,
                                                     mode_t mode)
{
    const Result<std::string> full_path = map_path(path);
    if (!full_path) {
        return copy_error(full_path);
    }

    int fd = ::open(full_path->c_str(), accesstype, mode);
    if (fd < 0) {
        return make_result(FAILED, errno);
    }

    return std::make_unique<LocalFile>(fd);
}

Result<std::unique_ptr<Dir>> LocalFilesystem::opendir(std::string_view path)
{
    const auto full_path = map_path(path);
    if (!full_path) {
        return copy_error(full_path);
    }

    DIR *fd = ::opendir(full_path->c_str());
    if (fd == nullptr) {
        return make_result(FAILED, errno);
    }

    return std::make_unique<LocalDir>(fd);
}

Result<Stat> LocalFilesystem::lstat(std::string_view path)
{
    const auto full_path = map_path(path);
    if (!full_path) {
        return copy_error(full_path);
    }

    struct stat buf{};
    if (::lstat(full_path->c_str(), &buf) < 0) {
        return make_result(FAILED, errno);
    }

    return from_os_stat(buf);
}

Result<std::string> LocalFilesystem::readlink(std::string_view path)
{
    // TODO: consider changing the interface of readlink to force the caller
    // to call lstat to avoid calling it twice for the same file during e.g.
    // handling of ::lookup (if we decide to always sync the link from the
    // remote on lstat-like operations)
    const auto full_path = map_path(path);
    if (!full_path) {
        return copy_error(full_path);
    }

    struct stat stat_buf{};
    if (::lstat(full_path->c_str(), &stat_buf) < 0) {
        return make_result(FAILED, errno);
    }

    std::string link_buf;
    // add one char to be able to see if the link grew in the meantime
    link_buf.resize(stat_buf.st_size + 1);
    ssize_t link_size = ::readlink(full_path->c_str(), link_buf.data(), link_buf.size());

    if (link_size > stat_buf.st_size) {
        // ??? meh.
    }

    link_buf.resize(link_size);
    return link_buf;
}

}
}
