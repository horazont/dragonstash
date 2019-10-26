#include <catch.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>

#include "cache/cache.hpp"

std::string random_name()
{
    std::random_device dev;
    std::mt19937_64 engine(dev());
    std::uniform_int_distribution<std::uint64_t> distribution;
    const std::uint64_t n1 = distribution(engine);
    std::stringstream ss;
    ss << std::hex << std::setw(sizeof(n1) * 2) << std::setfill('0') << std::right
       << n1;
    return ss.str();
}


std::filesystem::path custom_mkdtemp()
{
    const char *tmpdir = getenv("DRAGONSTASH_TEST_TMP_DIR");
    if (!tmpdir) {
        tmpdir = getenv("TMPDIR");
    }
    if (!tmpdir) {
        tmpdir = "/tmp";
    }
    std::filesystem::path basepath(tmpdir);
    std::filesystem::path final = basepath;
    int rc = 0;
    do {
        final /= "dragonstash-test-" + random_name();
        rc = mkdir(final.c_str(), 0700);
    } while (rc != 0 && errno == EEXIST);
    if (rc !=0) {
        throw std::runtime_error(
                    std::string("failed to create temporary directory: ") +
                    std::strerror(errno));
    }
    return final;
}


class TestEnvironment {
public:
    TestEnvironment():
        m_path(custom_mkdtemp())
    {

    }

    ~TestEnvironment()
    {
        std::filesystem::remove_all(m_path);
    }

private:
    const std::filesystem::path m_path;

public:
    inline const std::filesystem::path path() const {
        return m_path;
    }
};

SCENARIO("Inode serialization") {
    GIVEN("an Inode") {
        Dragonstash::Inode node{
            .mode = S_IFDIR,
            .size = 0x123456789abcdef0,
            .nblocks = 0x223456789abcdef0,
                    .uid = 0x12345678,
                    .gid = 0x12345679,
                    .atime = timespec{0x323456789abcdef0, 0x323456789abcdef1},
                    .mtime = timespec{0x423456789abcdef0, 0x423456789abcdef1},
                    .ctime = timespec{0x523456789abcdef0, 0x523456789abcdef1},
        };
        WHEN("serialized and deserialized") {
            std::array<std::uint8_t, Dragonstash::Inode::serialized_size> buf{};
            node.serialize(buf.data());
            auto parse_result = Dragonstash::Inode::parse(buf.data(), buf.size());

            THEN("parsing should succeed") {
                CHECK(parse_result);
                CHECK(parse_result.error() == 0);
            }

            THEN("the values should be identical") {
                CHECK(parse_result.error() == 0);
                REQUIRE(parse_result);
                CHECK(parse_result->mode == node.mode);
                CHECK(parse_result->size == node.size);
                CHECK(parse_result->nblocks == node.nblocks);
                CHECK(parse_result->uid == node.uid);
                CHECK(parse_result->gid == node.gid);
                CHECK(parse_result->atime.tv_sec == node.atime.tv_sec);
                CHECK(parse_result->atime.tv_nsec == node.atime.tv_nsec);
                CHECK(parse_result->mtime.tv_sec == node.mtime.tv_sec);
                CHECK(parse_result->mtime.tv_nsec == node.mtime.tv_nsec);
                CHECK(parse_result->ctime.tv_sec == node.ctime.tv_sec);
                CHECK(parse_result->ctime.tv_nsec == node.ctime.tv_nsec);
            }
        }
    }
}


SCENARIO("Inode caching")
{
    static constexpr Dragonstash::ino_t nonexistent_inode = (Dragonstash::ROOT_INO ^ 1) | 2;
    TestEnvironment env;
    GIVEN("An empty cache") {
        Dragonstash::Cache cache(env.path());
        WHEN("looking up an entry in the root") {
            auto entry = cache.Lookup(Dragonstash::ROOT_INO, "foo");
            THEN("the result is ENOENT") {
                REQUIRE(!entry);
                CHECK(entry.error() == -ENOENT);
            }
        }

        WHEN("stat-ing the root inode") {
            auto stat = cache.GetAttr(Dragonstash::ROOT_INO);
            THEN("the result is not an error") {
                REQUIRE(stat);
            }

            THEN("the mode indicates a directory") {
                REQUIRE(stat);

                CHECK((stat->mode & S_IFDIR) == S_IFDIR);
            }

            THEN("the size is zero") {
                REQUIRE(stat);

                CHECK(stat->size == 0);
            }

            THEN("the nblocks is zero") {
                REQUIRE(stat);

                CHECK(stat->nblocks == 0);
            }
        }

        WHEN("stat-ing a non-existent inode") {
            auto stat = cache.GetAttr(nonexistent_inode);
            THEN("the result is ENOENT") {
                REQUIRE(!stat);
                CHECK(stat.error() == -ENOENT);
            }
        }

        WHEN("iterating the root inode") {
            auto iter_result = cache.OpenDir(Dragonstash::ROOT_INO);
            THEN("the iterator should be returned") {
                REQUIRE(iter_result);
                CHECK(*iter_result);
            }

            THEN("no entries should come from the iterator") {
                REQUIRE(iter_result);
                REQUIRE(*iter_result);

                auto read_result = (*iter_result)->ReadDir();
                CHECK(!read_result);
                CHECK(read_result.error() == 0);
            }
        }

        WHEN("iterating a non-existent inode") {
            auto iter_result = cache.OpenDir(nonexistent_inode);
            THEN("it fails with -ENOENT") {
                REQUIRE(!iter_result);
                CHECK(iter_result.error() == -ENOENT);
            }
        }

        WHEN("calling PutAttr on a path in a directory inode") {
            Dragonstash::Backend::Stat info{
                .mode = S_IFDIR,
                        .size = 1234,
                        .uid = 1000,
                        .gid = 1000,
            };
            auto put_result = cache.PutAttr(Dragonstash::ROOT_INO,
                                            "bar",
                                            info);

            THEN("it succeeds and returns a new inode number") {
                REQUIRE(put_result);
                CHECK(*put_result != Dragonstash::ROOT_INO);
            }

            THEN("the path can be looked up") {
                REQUIRE(put_result);
                auto lookup_result = cache.Lookup(Dragonstash::ROOT_INO,
                                                  "bar");
                REQUIRE(lookup_result);
                CHECK(*lookup_result != *put_result);
            }

            THEN("the attributes can be read back") {
                REQUIRE(put_result);

                auto get_result = cache.GetAttr(*put_result);
                REQUIRE(get_result);
                CHECK(get_result->uid == info.uid);
                CHECK(get_result->gid == info.gid);
                CHECK(get_result->size == info.size);
            }
        }
    }
}
