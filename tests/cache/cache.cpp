#include <catch.hpp>

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <cstring>
#include <iostream>
#include <unistd.h>
#include <ctime>
#include <chrono>

#include "cache/cache.hpp"
#include "testutils/tempdir.hpp"


class TestSetup {
public:
    TestSetup():
        m_cache(m_env.path())
    {

    }

private:
    TemporaryDirectory m_env;
    Dragonstash::Cache m_cache;

public:
    inline TemporaryDirectory &env() {
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
        Dragonstash::InodeAttributes attr{
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
    Dragonstash::InodeAttributes attr{
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
    Dragonstash::InodeAttributes attr{
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

SCENARIO("Attributes retrieval and storage")
{
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();

    GIVEN("An empty cache") {
        WHEN("Fetching the attributes of an nonexistent inode") {
            auto getattr_result = cache.getattr(nonexistent_inode);

            THEN("It returns -ENOENT") {
                CHECK(!getattr_result);
                CHECK(getattr_result.error() == -ENOENT);
            }
        }

        WHEN("Fetching the attributes of the root inode") {
            auto getattr_result = cache.getattr(Dragonstash::ROOT_INO);

            THEN("It returns success") {
                CHECK(getattr_result);
                CHECK(getattr_result.error() == 0);
            }

            THEN("It is a directory") {
                REQUIRE(getattr_result);
                CHECK((getattr_result->mode & S_IFMT) == S_IFDIR);
            }

            THEN("Its UID and GID are the IDs of the current user") {
                REQUIRE(getattr_result);
                CHECK(getattr_result->uid == getuid());
                CHECK(getattr_result->gid == getgid());
            }

            THEN("Its timestamps are close to now") {
                REQUIRE(getattr_result);
                auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                CHECK((now - getattr_result->atime.tv_sec) < 10);
                CHECK((now - getattr_result->mtime.tv_sec) < 10);
                CHECK((now - getattr_result->ctime.tv_sec) < 10);
            }

            THEN("Its size is zero") {
                REQUIRE(getattr_result);
                CHECK(getattr_result->size == 0);
                CHECK(getattr_result->nblocks == 0);
            }
        }
    }

    Dragonstash::InodeAttributes attr{
        .mode = S_IFDIR
    };

    GIVEN("A cache with a single additional directory entry") {
        auto emplace_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr);
        CHECK(emplace_result.error() == 0);
        REQUIRE(emplace_result);

        WHEN("Requesting attributes of that entry") {
            auto getattr_result = cache.getattr(*emplace_result);
            CHECK(getattr_result.error() == 0);
            REQUIRE(getattr_result);

            THEN("The attributes from emplace have been preserved") {
                CHECK(getattr_result->mode == attr.mode);
                CHECK(getattr_result->gid == attr.gid);
                CHECK(getattr_result->uid == attr.uid);
                CHECK(getattr_result->size == attr.size);
                CHECK(getattr_result->nblocks == attr.nblocks);
                CHECK(getattr_result->atime.tv_sec == attr.atime.tv_sec);
                CHECK(getattr_result->atime.tv_nsec == attr.atime.tv_nsec);
                CHECK(getattr_result->mtime.tv_sec == attr.mtime.tv_sec);
                CHECK(getattr_result->mtime.tv_nsec == attr.mtime.tv_nsec);
                CHECK(getattr_result->ctime.tv_sec == attr.ctime.tv_sec);
                CHECK(getattr_result->ctime.tv_nsec == attr.ctime.tv_nsec);
            }

            THEN("The inode number in the result matches") {
                CHECK(getattr_result->ino == *emplace_result);
            }
        }

        WHEN("Replacing the directory entry") {
            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG,
            };
            auto emplace2_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            CHECK(emplace2_result.error() == 0);
            REQUIRE(emplace2_result);
            CHECK(*emplace2_result != *emplace_result);

            THEN("Calling getattr on the old entry fails with -ENOENT") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(!getattr_result);
                CHECK(getattr_result.error() == -ENOENT);
            }

            THEN("Calling getattr on the new entry succeeds") {
                auto getattr_result = cache.getattr(*emplace2_result);
                CHECK(getattr_result.error() == 0);
                REQUIRE(getattr_result);
                CHECK(getattr_result->mode == attr2.mode);
                CHECK(getattr_result->gid == attr2.gid);
                CHECK(getattr_result->uid == attr2.uid);
                CHECK(getattr_result->size == attr2.size);
                CHECK(getattr_result->nblocks == attr2.nblocks);
                CHECK(getattr_result->atime.tv_sec == attr2.atime.tv_sec);
                CHECK(getattr_result->atime.tv_nsec == attr2.atime.tv_nsec);
                CHECK(getattr_result->mtime.tv_sec == attr2.mtime.tv_sec);
                CHECK(getattr_result->mtime.tv_nsec == attr2.mtime.tv_nsec);
                CHECK(getattr_result->ctime.tv_sec == attr2.ctime.tv_sec);
                CHECK(getattr_result->ctime.tv_nsec == attr2.ctime.tv_nsec);
            }
        }
    }
}

SCENARIO("Locked inodes")
{
    GIVEN("A cache with an entry") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();
        Dragonstash::InodeAttributes attr{
            .mode = S_IFDIR
        };
        auto emplace_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr);
        CHECK(emplace_result.error() == 0);
        REQUIRE(emplace_result);

        WHEN("An inode is locked") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            THEN("The inode is not removed when the entry is replaced") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                REQUIRE(override_result);
                CHECK(*override_result != *emplace_result);

                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == 0);
                CHECK(getattr_result);
            }

            THEN("The inode has the invalid inode as parent") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                CHECK(override_result);

                auto parent_result = cache.parent(*emplace_result);
                CHECK(parent_result.error() == 0);
                CHECK(parent_result);
                CHECK(*parent_result == Dragonstash::INVALID_INO);
            }

            THEN("The inode has no name anymore") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                CHECK(override_result);

                auto name_result = cache.name(*emplace_result);
                CHECK(name_result.error() == 0);
                REQUIRE(name_result);
                CHECK(name_result->empty());
            }
        }

        WHEN("An inode is locked in the same transaction as its removal") {
            auto txn = cache.begin_rw();
            auto lock_result = txn.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            CHECK(lock_result);

            THEN("The inode is not removed") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = txn.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                REQUIRE(override_result);
                CHECK(*override_result != *emplace_result);

                CHECK(txn.commit());

                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == 0);
                CHECK(getattr_result);
            }
        }

        WHEN("A lock is rolled back") {
            auto txn = cache.begin_rw();
            auto lock_result = txn.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            CHECK(lock_result);
            txn.abort();

            THEN("The inode is removed when it is replaced") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                REQUIRE(override_result);
                CHECK(*override_result != *emplace_result);

                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == -ENOENT);
                CHECK(!getattr_result);
            }
        }

        WHEN("A removed, locked inode is unlocked") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            CHECK(override_result.error() == 0);
            REQUIRE(override_result);
            CHECK(*override_result != *emplace_result);

            auto release_result = cache.release(*emplace_result);
            CHECK(release_result.error() == 0);
            REQUIRE(release_result);

            THEN("The inode should be removed") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == -ENOENT);
                CHECK(!getattr_result);
            }
        }

        WHEN("A removed, locked inode is unlocked and the transaction is aborted") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            CHECK(override_result.error() == 0);
            REQUIRE(override_result);
            CHECK(*override_result != *emplace_result);

            auto txn = cache.begin_rw();
            auto release_result = txn.release(*emplace_result);
            CHECK(release_result.error() == 0);
            REQUIRE(release_result);
            txn.abort();

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == 0);
                CHECK(getattr_result);
            }
        }

        WHEN("A locked inode is unlocked but not removed") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            auto release_result = cache.release(*emplace_result);
            CHECK(release_result.error() == 0);
            REQUIRE(release_result);

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == 0);
                CHECK(getattr_result);
            }
        }

        WHEN("A locked inode is unlocked and then replaced") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            auto release_result = cache.release(*emplace_result);
            CHECK(release_result.error() == 0);
            REQUIRE(release_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            CHECK(override_result.error() == 0);
            REQUIRE(override_result);
            CHECK(*override_result != *emplace_result);

            THEN("The inode should be removed") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == -ENOENT);
                CHECK(!getattr_result);
            }
        }

        WHEN("A locked inode is unlocked, but rolled back, and then replaced") {
            auto lock_result = cache.lock(*emplace_result);
            CHECK(lock_result.error() == 0);
            REQUIRE(lock_result);

            auto txn = cache.begin_rw();
            auto release_result = txn.release(*emplace_result);
            CHECK(release_result.error() == 0);
            REQUIRE(release_result);
            txn.abort();

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            CHECK(override_result.error() == 0);
            REQUIRE(override_result);
            CHECK(*override_result != *emplace_result);

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                CHECK(getattr_result.error() == 0);
                CHECK(getattr_result);
            }
        }
    }
}

SCENARIO("Cross-process cache")
{
    GIVEN("A cache directory") {
        TemporaryDirectory env;
        WHEN("A cache is created with a single locked inode which has been replaced") {
            ino_t locked_inode = Dragonstash::INVALID_INO;

            {
                Dragonstash::Cache cache(env.path());
                Dragonstash::InodeAttributes attr{
                    .mode = S_IFDIR
                };
                auto emplace_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr);
                CHECK(emplace_result.error() == 0);
                REQUIRE(emplace_result);
                locked_inode = *emplace_result;

                auto lock_result = cache.lock(*emplace_result);
                CHECK(lock_result.error() == 0);
                REQUIRE(lock_result);

                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                CHECK(override_result.error() == 0);
                REQUIRE(override_result);
                CHECK(*override_result != *emplace_result);
            }

            THEN("The inode is gone after the cache has been restored") {
                Dragonstash::Cache cache(env.path());
                auto getattr_result = cache.getattr(locked_inode);
                CHECK(getattr_result.error() == -ENOENT);
                CHECK(!getattr_result);
            }
        }
    }
}

SCENARIO("Storage and retrieval of symlinks") {
    GIVEN("An empty cache") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        WHEN("Calling readlink on a nonexistent inode") {
            auto read_result = cache.readlink(nonexistent_inode);

            THEN("It returns -ENOENT") {
                CHECK(read_result.error() == -ENOENT);
                CHECK(!read_result);
            }
        }

        WHEN("Calling writelink on a nonexistent inode") {
            auto write_result = cache.writelink(nonexistent_inode, "/foo");

            THEN("It returns -ENOENT") {
                CHECK(write_result.error() == -ENOENT);
                CHECK(!write_result);
            }
        }
    }

    GIVEN("A cache with a dir and a link") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();
        Dragonstash::InodeAttributes dir_attrs{
            .mode = S_IFDIR,
        };
        Dragonstash::InodeAttributes link_attrs{
            .mode = S_IFLNK,
        };

        auto dir_result = cache.emplace(Dragonstash::ROOT_INO,
                                        "dir", dir_attrs);
        auto lnk_result = cache.emplace(Dragonstash::ROOT_INO,
                                        "lnk", link_attrs);

        CHECK(dir_result.error() == 0);
        CHECK(dir_result);
        CHECK(lnk_result.error() == 0);
        CHECK(lnk_result);

        WHEN("Writing a destination to the link") {
            auto write_result = cache.writelink(*lnk_result, "/foo");

            THEN("It succeeds") {
                CHECK(write_result.error() == 0);
                CHECK(write_result);
            }
        }

        WHEN("Calling writelink on a directory") {
            auto write_result = cache.writelink(*dir_result, "/foo");

            THEN("It fails with -EINVAL") {
                CHECK(write_result.error() == -EINVAL);
                CHECK(!write_result);
            }
        }

        WHEN("Calling readlink on a directory") {
            auto read_result = cache.readlink(*dir_result);

            THEN("It fails with -EINVAL") {
                CHECK(read_result.error() == -EINVAL);
                CHECK(!read_result);
            }
        }

        WHEN("Having written a destination on a link") {
            auto write_result = cache.writelink(*lnk_result, "/foo");
            CHECK(write_result.error() == 0);
            REQUIRE(write_result);

            THEN("It can be read back") {
                auto read_result = cache.readlink(*lnk_result);
                CHECK(read_result.error() == 0);
                REQUIRE(read_result);
                CHECK(*read_result == "/foo");
            }
        }

        WHEN("Replacing a link with a file") {
            auto write_result = cache.writelink(*lnk_result, "/foo");
            CHECK(write_result.error() == 0);
            REQUIRE(write_result);

            Dragonstash::InodeAttributes file_attrs = {
                .mode = S_IFREG,
            };

            auto emplace_result = cache.emplace(Dragonstash::ROOT_INO,
                                                "lnk", file_attrs);
            CHECK(emplace_result.error() == 0);
            REQUIRE(emplace_result);
            CHECK(*emplace_result != *lnk_result);

            THEN("The link is not readable anymore") {
                auto read_result = cache.readlink(*lnk_result);
                CHECK(read_result.error() == -ENOENT);
                CHECK(!read_result);
            }
        }
    }
}

SCENARIO("Nested directories") {
    GIVEN("A cache with a directory") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();
        Dragonstash::InodeAttributes dir1_attrs{
            .mode = S_IFDIR,
        };

        auto dir1_result = cache.emplace(Dragonstash::ROOT_INO,
                                         "dir", dir1_attrs);
        CHECK(dir1_result.error() == 0);
        CHECK(dir1_result);

        WHEN("Emplacing a directory inside the first one") {
            auto dir2_result = cache.emplace(*dir1_result,
                                             "subdir", dir1_attrs);
            CHECK(dir2_result.error() == 0);
            CHECK(dir2_result);

            THEN("It is not visible in the root directory") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "subdir");
                CHECK(lookup_result.error() == -ENOENT);
                CHECK(!lookup_result);
            }

            THEN("It is visible in the first directory") {
                auto lookup_result = cache.lookup(*dir1_result,
                                                  "subdir");
                CHECK(lookup_result.error() == 0);
                CHECK(*lookup_result == *dir2_result);
            }
        }

        WHEN("Emplacing a file over a directory with a child") {
            auto dir2_result = cache.emplace(*dir1_result,
                                             "subdir", dir1_attrs);
            CHECK(dir2_result.error() == 0);
            CHECK(dir2_result);

            Dragonstash::InodeAttributes file_attrs{
                .mode = S_IFREG,
            };

            auto emplace_result = cache.emplace(Dragonstash::ROOT_INO,
                                                "dir", file_attrs);
            CHECK(emplace_result.error() == 0);
            CHECK(emplace_result);

            THEN("The subdirectory is gone") {
                auto lookup_result = cache.lookup(*dir1_result, "subdir");
                CHECK(lookup_result.error() == -ENOENT);
                CHECK(!lookup_result);

                auto getattr_result = cache.getattr(*dir2_result);
                CHECK(getattr_result.error() == -ENOENT);
                CHECK(!getattr_result);
            }
        }
    }
}
