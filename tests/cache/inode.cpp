#include <catch.hpp>

#include "cache/inode.hpp"

SCENARIO("Inode deserialization") {
    GIVEN("an invalid header") {
        WHEN("the buffer is too short") {
            std::array<std::uint8_t, 0> buf{};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0") {
            std::array<std::uint8_t, 1> buf{{0x00}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0x02") {
            std::array<std::uint8_t, 1> buf{{0x02}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }

        WHEN("the buffer has the invalid version 0xff") {
            std::array<std::uint8_t, 1> buf{{0xff}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }
    }

    GIVEN("a version 1 buffer") {
        WHEN("the buffer is empty after header") {
            std::array<std::uint8_t, 1> buf{{0x01}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }

        WHEN("the buffer is too short") {
            std::array<std::uint8_t, Dragonstash::Inode::serialized_size-1> buf{{0x01}};
            THEN("return -EINVAL") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(!parse_result);
                CHECK(parse_result.error() == -EINVAL);
            }
        }

        WHEN("the buffer contains a valid inode") {
            std::array<std::uint8_t, Dragonstash::Inode::serialized_size> buf{{
                    0x01,
                    0x11, 0x22, 0x33, 0x44, // mode
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
                                                                  }};
            THEN("return proper inode object") {
                auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());
                CHECK(parse_result.error() == 0);
                REQUIRE(parse_result);
                CHECK(parse_result->mode == 0x44332211);
                CHECK(parse_result->size == 0x8070605040302010);
                CHECK(parse_result->nblocks == 0x8171615141312111);
                CHECK(parse_result->uid == 0x15141312);
                CHECK(parse_result->gid == 0x25242322);
                CHECK(parse_result->atime.tv_sec == 0x0000000000000010);
                CHECK(parse_result->atime.tv_nsec == 0x0000000000000020);
                CHECK(parse_result->mtime.tv_sec == 0x1000000000000010);
                CHECK(parse_result->mtime.tv_nsec == 0x0000000010000020);
                CHECK(parse_result->ctime.tv_sec == 0x2000000000000010);
                CHECK(parse_result->ctime.tv_nsec == 0x0000000020000020);
            }
        }
    }
}
