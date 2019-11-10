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

Result<Inode> Inode::parse(const std::uint8_t *buf, size_t sz)
{
    if (sz < 1) {
        return make_result(FAILED, -EINVAL);
    }

    uint8_t version;
    memcpy(&version, buf, sizeof(version));

    switch (version)
    {
    case 1:
        return parse_v1(&buf[1], sz-1);
    default:
        return make_result(FAILED, -EINVAL);
    }
}

void Inode::serialize(uint8_t *buf)
{
    static const std::uint8_t version = 0x01;
    put(buf, version);
    put(buf, parent);
    put(buf, mode);
    put(buf, size);
    put(buf, nblocks);
    put(buf, uid);
    put(buf, gid);
    put(buf, atime.tv_sec);
    put(buf, atime.tv_nsec);
    put(buf, mtime.tv_sec);
    put(buf, mtime.tv_nsec);
    put(buf, ctime.tv_sec);
    put(buf, ctime.tv_nsec);
}

Result<Inode> Inode::parse_v1(const std::uint8_t *buf, size_t sz)
{
    Inode result{};
    if (!scan(buf, sz, result.parent)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.mode)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.size)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.nblocks)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.uid)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.gid)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.atime.tv_sec)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.atime.tv_nsec)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.mtime.tv_sec)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.mtime.tv_nsec)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.ctime.tv_sec)) return make_result(FAILED, -EINVAL);
    if (!scan(buf, sz, result.ctime.tv_nsec)) return make_result(FAILED, -EINVAL);
    return result;
}

}
