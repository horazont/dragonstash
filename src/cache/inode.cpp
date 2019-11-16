/**********************************************************************
File name: inode.cpp
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
#include "cache/inode.hpp"

#include <cstring>

namespace Dragonstash {

template <typename T>
bool scan(const std::uint8_t *&buf, size_t &sz, T &out)
{
    if (sz < sizeof(T)) {
        return false;
    }

    memcpy(&out, buf, sizeof(T));
    sz -= sizeof(T);
    buf += sizeof(T);
    return true;
}

template <typename T>
void put(std::uint8_t *&buf, T in)
{
    memcpy(buf, &in, sizeof(T));
    buf += sizeof(T);
}

Result<copyfree_wrap<InodeV1> > InodeV1::parse_inplace(std::basic_string_view<std::byte> buf)
{
    if (buf.empty()) {
        return make_result(FAILED, EINVAL);
    }
    const auto version = static_cast<std::uint8_t>(buf[0]);
    if (version != 1) {
        return make_result(FAILED, EINVAL);
    }

    if (buf.size() < INODE_SIZE) {
        return make_result(FAILED, EINVAL);
    }

    return copyfree_wrap<InodeV1>(*reinterpret_cast<const InodeV1*>(buf.data()));
}

}
