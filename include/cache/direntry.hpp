/**********************************************************************
File name: direntry.hpp
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
#ifndef DRAGONSTASH_CACHE_DIRENTRY_H
#define DRAGONSTASH_CACHE_DIRENTRY_H

#include <cstdint>

#include "inode.hpp"

namespace Dragonstash {

enum class DirEntryFlag {
    REWRITE_DELETE_CANDIDATE = 0,
};


struct DirEntryV1 {
    std::uint8_t version;
    std::uint8_t _reserved0;
    std::uint16_t flags;
    std::uint32_t mode;
    ino_t entry_ino;

    [[nodiscard]] static Result<std::tuple<copyfree_wrap<DirEntryV1>, std::string_view>> parse_inplace(std::basic_string_view<std::byte> buf);

    [[nodiscard]] inline static Result<std::tuple<DirEntryV1, std::string>> parse(std::basic_string_view<std::byte> buf)
    {
        auto result = parse_inplace(buf);
        if (!result) {
            return copy_error(result);
        }
        return make_result(std::make_tuple(std::get<0>(*result).extract(),
                                           std::string(std::get<1>(*result))));
    }
};

using DirEntry = DirEntryV1;

static constexpr std::size_t DIR_ENTRY_SIZE = sizeof(DirEntry);
static constexpr std::uint8_t DIR_ENTRY_VERSION = 1;

// TODO: replace char* by std::span<char> once we have it
[[nodiscard]] inline std::tuple<DirEntry*, char*> emplace(
        std::basic_string<std::byte> &buf,
        std::size_t name_size,
        ino_t entry_ino = INVALID_INO)
{
    buf.resize(DIR_ENTRY_SIZE + name_size);
    DirEntry &dir_entry = *reinterpret_cast<DirEntry*>(buf.data());
    dir_entry.version = DIR_ENTRY_VERSION;
    dir_entry.entry_ino = entry_ino;
    return std::make_tuple(
                &dir_entry,
                reinterpret_cast<char*>(std::data(buf)) + DIR_ENTRY_SIZE);
}

inline DirEntry mkdirentry(ino_t entry_ino)
{
    return DirEntry{
        .version = DIR_ENTRY_VERSION,
        .entry_ino = entry_ino,
    };
}


}

#endif
