/**********************************************************************
File name: direntry.cpp
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
#include "cache/direntry.hpp"

#include <cassert>

namespace Dragonstash {

Result<std::tuple<copyfree_wrap<DirEntry>, std::string_view> > DirEntry::parse_inplace(std::basic_string_view<std::byte> buf)
{
    if (buf.empty()) {
        return make_result(FAILED, EINVAL);
    }

    const auto version = static_cast<std::uint8_t>(buf[0]);
    if (version != 1) {
        return make_result(FAILED, EINVAL);
    }

    if (buf.size() < DIR_ENTRY_SIZE) {
        return make_result(FAILED, EINVAL);
    }

    std::string_view name_view(reinterpret_cast<const char*>(buf.data()), buf.size());
    name_view.remove_prefix(DIR_ENTRY_SIZE);

    // alignment isn’t fulfilled, we have to use copy
    if (reinterpret_cast<std::size_t>(buf.data()) % alignof(DirEntryV1) != 0) {
        DirEntry entry;
        memcpy(&entry, buf.data(), DIR_ENTRY_SIZE);
        return std::make_tuple(
                    copyfree_wrap<DirEntry>(std::move(entry)),
                    name_view);
    }
    return std::make_tuple(
                copyfree_wrap(*reinterpret_cast<const DirEntry*>(buf.data())),
                name_view
                );
}

}
