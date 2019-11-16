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
#include <catch2/catch.hpp>

#include <sys/stat.h>

#include "cache/inode.hpp"

SCENARIO("Inode serialization") {
    GIVEN("an Inode") {
        Dragonstash::Inode node = mkinode(
            Dragonstash::InodeAttributes{
                Dragonstash::CommonFileAttributes{
                    .size = 0x123456789abcdef0,
                    .nblocks = 0x223456789abcdef0,
                    .uid = 0x12345678,
                    .gid = 0x12345679,
                    .atime = timespec{0x323456789abcdef0, 0x323456789abcdef1},
                    .mtime = timespec{0x423456789abcdef0, 0x423456789abcdef1},
                    .ctime = timespec{0x523456789abcdef0, 0x523456789abcdef1},
                },
                S_IFDIR,
            },
            0x1122334455667788
        );
        WHEN("serialized and deserialized") {
            const auto buf = Dragonstash::serialize(node);
            auto parse_result = Dragonstash::Inode::parse(buf);

            THEN("parsing should succeed") {
                CHECK(parse_result);
                CHECK(parse_result.error() == 0);
            }

            THEN("the values should be identical") {
                CHECK(parse_result.error() == 0);
                REQUIRE(parse_result);
                CHECK(parse_result->parent == node.parent);
                CHECK(parse_result->attr.mode == node.attr.mode);
                CHECK(parse_result->attr.common.size == node.attr.common.size);
                CHECK(parse_result->attr.common.nblocks == node.attr.common.nblocks);
                CHECK(parse_result->attr.common.uid == node.attr.common.uid);
                CHECK(parse_result->attr.common.gid == node.attr.common.gid);
                CHECK(parse_result->attr.common.atime.tv_sec == node.attr.common.atime.tv_sec);
                CHECK(parse_result->attr.common.atime.tv_nsec == node.attr.common.atime.tv_nsec);
                CHECK(parse_result->attr.common.mtime.tv_sec == node.attr.common.mtime.tv_sec);
                CHECK(parse_result->attr.common.mtime.tv_nsec == node.attr.common.mtime.tv_nsec);
                CHECK(parse_result->attr.common.ctime.tv_sec == node.attr.common.ctime.tv_sec);
                CHECK(parse_result->attr.common.ctime.tv_nsec == node.attr.common.ctime.tv_nsec);
            }
        }
    }
}

SCENARIO("Inode deserialization") {
    GIVEN("an invalid header") {
        WHEN("the buffer is too short") {
            std::array<std::byte, 0> buf{};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(buf.data(), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0") {
            std::array<std::uint8_t, 1> buf{{0x00}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0x02") {
            std::array<std::uint8_t, 1> buf{{0x02}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0xff") {
            std::array<std::uint8_t, 1> buf{{0xff}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }
    }

    GIVEN("a version 1 buffer") {
        WHEN("the buffer is empty after header") {
            std::array<std::uint8_t, 1> buf{{0x01}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }

        WHEN("the buffer is too short") {
            std::array<std::uint8_t, Dragonstash::INODE_SIZE-1> buf{{0x01}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(!parse_result);
                CHECK(parse_result.error() == EINVAL);
            }
        }

        WHEN("the buffer contains a valid inode v1") {
            std::array<std::uint8_t, Dragonstash::INODE_SIZE> buf{{
                    0x01, // version
                    0x00, // _reserved0
                    0x00, 0x00, // _reserved1
                    0x00, 0x00, 0x00, 0x00, // _reserved2
                    0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, // parent
                    0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, // size
                    0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71, 0x81, // nblocks
                    0x12, 0x13, 0x14, 0x15, // uid
                    0x22, 0x23, 0x24, 0x25, // gid
                    0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // atime.tv_sec
                    0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // atime.tv_nsec
                    0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, // mtime.tv_sec
                    0x20, 0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, // mtime.tv_nsec
                    0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, // ctime.tv_sec
                    0x20, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, // ctime.tv_nsec
                    0x11, 0x22, 0x33, 0x44, // mode
                                                                  }};
            THEN("return proper inode object") {
                auto parse_result = Dragonstash::Inode::parse(std::basic_string_view<std::byte>(reinterpret_cast<std::byte*>(buf.data()), buf.size()));
                CHECK(parse_result.error() == 0);
                REQUIRE(parse_result);
                CHECK(parse_result->parent == 0xf0debc9a78563412);
                CHECK(parse_result->attr.mode == 0x44332211);
                CHECK(parse_result->attr.common.size == 0x8070605040302010);
                CHECK(parse_result->attr.common.nblocks == 0x8171615141312111);
                CHECK(parse_result->attr.common.uid == 0x15141312);
                CHECK(parse_result->attr.common.gid == 0x25242322);
                CHECK(parse_result->attr.common.atime.tv_sec == 0x0000000000000010);
                CHECK(parse_result->attr.common.atime.tv_nsec == 0x0000000000000020);
                CHECK(parse_result->attr.common.mtime.tv_sec == 0x1000000000000010);
                CHECK(parse_result->attr.common.mtime.tv_nsec == 0x0000000010000020);
                CHECK(parse_result->attr.common.ctime.tv_sec == 0x2000000000000010);
                CHECK(parse_result->attr.common.ctime.tv_nsec == 0x0000000020000020);
            }
        }
    }
}
