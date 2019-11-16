/**********************************************************************
File name: inode.hpp
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
#ifndef DRAGONSTASH_CACHE_INODE_H
#define DRAGONSTASH_CACHE_INODE_H

#include <cstdint>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <string_view>

#if __cpp_lib_span >= 201803L
#include <span>
#endif

#include "error.hpp"
#include "backend.hpp"
#include "cache/common.hpp"

namespace Dragonstash {

using ino_t = std::uint64_t;

static constexpr ino_t INVALID_INO = 0;
static constexpr ino_t ROOT_INO = 1;

struct CommonFileAttributes {
    std::uint64_t size;
    std::uint64_t nblocks;
    std::uint32_t uid;
    std::uint32_t gid;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
};

static_assert(std::is_pod_v<CommonFileAttributes>);

struct InodeAttributes {
    CommonFileAttributes common;
    std::uint32_t mode;

    static InodeAttributes from_backend_stat(const Backend::Stat &attr)
    {
        return InodeAttributes{
            .common = CommonFileAttributes{
                .size = attr.size,
                .nblocks = 0,
                .uid = attr.uid,
                .gid = attr.gid,
                .atime = attr.atime,
                .mtime = attr.mtime,
                .ctime = attr.ctime
            },
            .mode = attr.mode
        };
    }
};

static_assert(std::is_pod_v<InodeAttributes>);

struct Stat {
    InodeAttributes attr;
    ino_t ino;

    inline operator struct stat() const {
        struct stat result{};
        result.st_ino = ino;
        result.st_mode = attr.mode;
        result.st_nlink = 1;
        result.st_uid = attr.common.uid;
        result.st_gid = attr.common.gid;
        result.st_size = off_t(attr.common.size);
        result.st_blksize = CACHE_PAGE_SIZE;
        result.st_blocks = blkcnt_t(attr.common.nblocks);
        result.st_atim = attr.common.atime;
        result.st_mtim = attr.common.mtime;
        result.st_ctim = attr.common.ctime;
        return result;
    }
};

static_assert(std::is_pod_v<Stat>);

struct DirectoryEntry: public Stat {
    std::string name;
    bool complete;
};

enum class InodeFlag {
    /**
     * @brief Indicate that the inode has been fully synced from the source at
     * least once.
     *
     * This is currently only used for directories. If the flag is set, the
     * filesystem interface can confidently return ENOENT for entries which
     * cannot be looked up in the directory. Otherwise, it has to return EIO,
     * because the entries may either not exist on the source or may not have
     * been synced yet.
     */
    SYNCED = 0,
};

struct InodeV1 {
    std::uint8_t version;
    std::uint8_t _reserved0;
    std::uint16_t flags;
    std::uint32_t _reserved2;
    ino_t parent;
    InodeAttributes attr;

    static Result<copyfree_wrap<InodeV1>> parse_inplace(std::basic_string_view<std::byte> buf);

    static inline Result<InodeV1> parse(std::basic_string_view<std::byte> buf) {
        auto result = parse_inplace(buf);
        if (!result) {
            return copy_error(result);
        }
        return result->extract();
    }

#if __cpp_lib_span >= 201803L
    static inline Result<copyfree_wrap<InodeV1>> parse_inplace(std::span<const std::byte> buf) {
        return parse_inplace(std::basic_string_view<std::byte>(buf.data(), buf.size());
    }

    static inline Result<InodeV1> parse(std::span<const std::byte> buf) {
        auto result = parse_inplace(buf);
        if (!result) {
            return copy_error(result);
        }
        return result->extract();
    }
#endif

    [[nodiscard]] inline bool test_flag(InodeFlag flag) const {
        return (flags & (1<<static_cast<unsigned>(flag))) != 0;
    }

    inline void set_flag(InodeFlag flag, bool presence = true) {
        if (presence) {
            flags |= (1<<static_cast<unsigned>(flag));
        } else {
            flags &= ~static_cast<decltype(flags)>(1<<static_cast<unsigned>(flag));
        }
    }


};

using Inode = InodeV1;

static_assert(std::is_pod_v<Inode>);

static constexpr std::size_t INODE_SIZE = sizeof(Inode);
static constexpr std::size_t INODE_CURRENT_VERSION = 1;

template <typename T>
inline Inode mkinode(T &&attr, ino_t parent = INVALID_INO) {
    return Inode{
        .version = INODE_CURRENT_VERSION,
        .parent = parent,
        .attr = std::forward<T>(attr),
    };
}

inline void serialize(const Inode &inode, std::byte *buf) {
    memcpy(buf, &inode, INODE_SIZE);
}

template <typename T, typename _ = typename std::enable_if<sizeof(T) == 1 && std::is_arithmetic_v<T>>::type>
inline std::basic_string<T> serialize_as(const Inode &inode) {
    std::basic_string<T> buf;
    buf.resize(INODE_SIZE);
    serialize(inode, reinterpret_cast<std::byte*>(buf.data()));
    return buf;
}

inline std::basic_string<std::byte> serialize(const Inode &inode) {
    std::basic_string<std::byte> buf;
    buf.resize(INODE_SIZE);
    serialize(inode, buf.data());
    return buf;
}

}

#endif
