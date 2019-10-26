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


class TestSetup {
public:
    TestSetup():
        m_cache(m_env.path())
    {

    }

private:
    TestEnvironment m_env;
    Dragonstash::Cache m_cache;

public:
    inline TestEnvironment &env() {
        return m_env;
    }

    inline Dragonstash::Cache &cache() {
        return m_cache;
    }
};

static constexpr ino_t nonexistent_inode = (Dragonstash::ROOT_INO | 2) ^ 1;

SCENARIO("Inode cache behaviour")
{
    TestSetup setup;

    GIVEN("An empty cache") {
        Dragonstash::Cache &cache = setup.cache();

        WHEN("looking up a name on the root inode") {
            auto result = cache.lookup(Dragonstash::ROOT_INO, "foo");

            THEN("it returns -ENOENT") {
                CHECK(!result);
                CHECK(result.error() == -ENOENT);
            }
        }

        WHEN("looking up a name on the invalid inode") {
            auto result = cache.lookup(Dragonstash::INVALID_INO, "foo");

            THEN("it returns -EINVAL") {
                CHECK(!result);
                CHECK(result.error() == -EINVAL);
            }
        }

        WHEN("looking up a name on a nonexistent inode") {
            auto result = cache.lookup(nonexistent_inode, "foo");

            THEN("it returns -ENOENT") {
                CHECK(!result);
                CHECK(result.error() == -ENOENT);
            }
        }
    }

    GIVEN("An empty cache and directory attributes") {
        Dragonstash::Cache &cache = setup.cache();
        Dragonstash::Backend::Stat attr{
            .mode = S_IFDIR
        };

        WHEN("emplacing the attributes in the root inode") {
            auto result = cache.emplace(Dragonstash::ROOT_INO, "foo",
                                        attr);

            THEN("the cache returns a new, valid inode number") {
                REQUIRE(result);
                CHECK(*result != Dragonstash::ROOT_INO);
                CHECK(*result != Dragonstash::INVALID_INO);
            }

            THEN("the entry should be found using lookup") {
                auto lookup_result = cache.lookup(
                            Dragonstash::ROOT_INO,
                            "foo");
                REQUIRE(result);
                CHECK(lookup_result.error() == 0);
                REQUIRE(lookup_result);
                CHECK(*result == *lookup_result);
            }

            THEN("no other entries should have appeared magically") {
                auto lookup_result = cache.lookup(
                            Dragonstash::ROOT_INO,
                            "bar");
                CHECK(!lookup_result);
                CHECK(lookup_result.error() == -ENOENT);
            }
        }
    }
}

SCENARIO("Directory entry replacement")
{
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();
    Dragonstash::Backend::Stat attr{
        .mode = S_IFDIR
    };

    GIVEN("A directory with two entries") {
        auto entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                          "entry",
                                          attr);
        REQUIRE(entry_result);
        ino_t entry_ino = *entry_result;

        auto second_entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                                 "other",
                                                 attr);
        REQUIRE(second_entry_result);
        ino_t second_entry_ino = *second_entry_result;

        WHEN("Emplacing a new inode with a conflicting name") {
            auto new_result = cache.emplace(Dragonstash::ROOT_INO,
                                            "entry",
                                            attr);
            THEN("A new inode is returned") {
                REQUIRE(new_result);
                CHECK(*new_result != entry_ino);
            }

            THEN("Lookup returns the new inode only") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "entry");
                REQUIRE(lookup_result);
                CHECK(*lookup_result == *new_result);
            }

            THEN("The second entry is unharmed") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "other");
            }
        }
    }
}

SCENARIO("Reverse lookup")
{
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();
    Dragonstash::Backend::Stat attr{
        .mode = S_IFDIR
    };

    GIVEN("An empty cache") {
        WHEN("Looking up the name of the root inode") {
            THEN("The name is empty") {
                auto lookup_result = cache.name(Dragonstash::ROOT_INO);
                REQUIRE(lookup_result);
                CHECK(lookup_result->empty());
            }
        }

        WHEN("Looking up the name of a non-existent inode") {
            THEN("-ENOENT is returned") {
                auto lookup_result = cache.name(nonexistent_inode);
                CHECK(!lookup_result);
                CHECK(lookup_result.error() == -ENOENT);
            }
        }
    }

    GIVEN("A directory with an entry") {
        auto entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                          "entry",
                                          attr);
        REQUIRE(entry_result);
        ino_t entry_ino = *entry_result;

        WHEN("Looking up the name of the inode") {
            auto lookup_result = cache.name(entry_ino);

            THEN("The correct name is returned") {
                REQUIRE(lookup_result);
                CHECK(*lookup_result == "entry");
            }
        }
    }
}
