/**********************************************************************
File name: cache.cpp
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
#include "testutils/result.hpp"


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
                check_result_error(result, ENOENT);
            }
        }

        WHEN("looking up a name on the invalid inode") {
            auto result = cache.lookup(Dragonstash::INVALID_INO, "foo");

            THEN("it returns -EINVAL") {
                check_result_error(result, EINVAL);
            }
        }

        WHEN("looking up a name on a nonexistent inode") {
            auto result = cache.lookup(nonexistent_inode, "foo");

            THEN("it returns -ENOENT") {
                check_result_error(result, ENOENT);
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
                require_result_ok(result);
                CHECK(*result != Dragonstash::ROOT_INO);
                CHECK(*result != Dragonstash::INVALID_INO);
            }

            THEN("the entry should be found using lookup") {
                auto lookup_result = cache.lookup(
                            Dragonstash::ROOT_INO,
                            "foo");
                require_result_ok(result);
                require_result_ok(lookup_result);
                CHECK(*result == *lookup_result);
            }

            THEN("no other entries should have appeared magically") {
                auto lookup_result = cache.lookup(
                            Dragonstash::ROOT_INO,
                            "bar");
                check_result_error(lookup_result, ENOENT);
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
    Dragonstash::InodeAttributes other_attr{
        .mode = S_IFREG
    };

    GIVEN("A directory with two entries") {
        auto entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                          "entry",
                                          attr);
        require_result_ok(entry_result);
        ino_t entry_ino = *entry_result;

        auto second_entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                                 "other",
                                                 attr);
        require_result_ok(second_entry_result);
        ino_t second_entry_ino = *second_entry_result;

        WHEN("Emplacing a new inode with a conflicting name and equal format") {
            auto new_result = cache.emplace(Dragonstash::ROOT_INO,
                                            "entry",
                                            attr);
            THEN("The same inode is returned") {
                require_result_ok(new_result);
                CHECK(*new_result == entry_ino);
            }

            THEN("The second entry is unharmed") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "other");
            }
        }

        WHEN("Emplacing a new inode with a conflicting name and format") {
            auto new_result = cache.emplace(Dragonstash::ROOT_INO,
                                            "entry",
                                            other_attr);
            THEN("A new inode is returned") {
                require_result_ok(new_result);
                CHECK(*new_result != entry_ino);
            }

            THEN("Lookup returns the new inode only") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "entry");
                require_result_ok(lookup_result);
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
                require_result_ok(lookup_result);
                CHECK(lookup_result->empty());
            }
        }

        WHEN("Looking up the name of a non-existent inode") {
            THEN("-ENOENT is returned") {
                auto lookup_result = cache.name(nonexistent_inode);
                check_result_error(lookup_result, ENOENT);
            }
        }
    }

    GIVEN("A directory with an entry") {
        auto entry_result = cache.emplace(Dragonstash::ROOT_INO,
                                          "entry",
                                          attr);
        require_result_ok(entry_result);
        ino_t entry_ino = *entry_result;

        WHEN("Looking up the name of the inode") {
            auto lookup_result = cache.name(entry_ino);

            THEN("The correct name is returned") {
                require_result_ok(lookup_result);
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
                check_result_error(getattr_result, ENOENT);
            }
        }

        WHEN("Fetching the attributes of the root inode") {
            auto getattr_result = cache.getattr(Dragonstash::ROOT_INO);

            THEN("It returns success") {
                check_result_ok(getattr_result);
            }

            THEN("It is a directory") {
                require_result_ok(getattr_result);
                CHECK((getattr_result->attr.mode & S_IFMT) == S_IFDIR);
            }

            THEN("Its UID and GID are the IDs of the current user") {
                require_result_ok(getattr_result);
                CHECK(getattr_result->attr.common.uid == getuid());
                CHECK(getattr_result->attr.common.gid == getgid());
            }

            THEN("Its timestamps are close to now") {
                require_result_ok(getattr_result);
                auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                CHECK((now - getattr_result->attr.common.atime.tv_sec) < 10);
                CHECK((now - getattr_result->attr.common.mtime.tv_sec) < 10);
                CHECK((now - getattr_result->attr.common.ctime.tv_sec) < 10);
            }

            THEN("Its size is zero") {
                require_result_ok(getattr_result);
                CHECK(getattr_result->attr.common.size == 0);
                CHECK(getattr_result->attr.common.nblocks == 0);
            }
        }
    }

    Dragonstash::InodeAttributes attr{
        .mode = S_IFDIR
    };

    GIVEN("A cache with a single additional directory entry") {
        auto emplace_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr);
        require_result_ok(emplace_result);

        WHEN("Requesting attributes of that entry") {
            auto getattr_result = cache.getattr(*emplace_result);
            require_result_ok(getattr_result);

            THEN("The attributes from emplace have been preserved") {
                CHECK(getattr_result->attr.mode == attr.mode);
                CHECK(getattr_result->attr.common.gid == attr.common.gid);
                CHECK(getattr_result->attr.common.uid == attr.common.uid);
                CHECK(getattr_result->attr.common.size == attr.common.size);
                CHECK(getattr_result->attr.common.nblocks == attr.common.nblocks);
                CHECK(getattr_result->attr.common.atime.tv_sec == attr.common.atime.tv_sec);
                CHECK(getattr_result->attr.common.atime.tv_nsec == attr.common.atime.tv_nsec);
                CHECK(getattr_result->attr.common.mtime.tv_sec == attr.common.mtime.tv_sec);
                CHECK(getattr_result->attr.common.mtime.tv_nsec == attr.common.mtime.tv_nsec);
                CHECK(getattr_result->attr.common.ctime.tv_sec == attr.common.ctime.tv_sec);
                CHECK(getattr_result->attr.common.ctime.tv_nsec == attr.common.ctime.tv_nsec);
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
            require_result_ok(emplace2_result);
            CHECK(*emplace2_result != *emplace_result);

            THEN("Calling getattr on the old entry fails with -ENOENT") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_error(getattr_result, ENOENT);
            }

            THEN("Calling getattr on the new entry succeeds") {
                auto getattr_result = cache.getattr(*emplace2_result);
                require_result_ok(getattr_result);
                CHECK(getattr_result->attr.mode == attr2.mode);
                CHECK(getattr_result->attr.common.gid == attr2.common.gid);
                CHECK(getattr_result->attr.common.uid == attr2.common.uid);
                CHECK(getattr_result->attr.common.size == attr2.common.size);
                CHECK(getattr_result->attr.common.nblocks == attr2.common.nblocks);
                CHECK(getattr_result->attr.common.atime.tv_sec == attr2.common.atime.tv_sec);
                CHECK(getattr_result->attr.common.atime.tv_nsec == attr2.common.atime.tv_nsec);
                CHECK(getattr_result->attr.common.mtime.tv_sec == attr2.common.mtime.tv_sec);
                CHECK(getattr_result->attr.common.mtime.tv_nsec == attr2.common.mtime.tv_nsec);
                CHECK(getattr_result->attr.common.ctime.tv_sec == attr2.common.ctime.tv_sec);
                CHECK(getattr_result->attr.common.ctime.tv_nsec == attr2.common.ctime.tv_nsec);
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
        require_result_ok(emplace_result);

        WHEN("An inode is locked") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            THEN("The inode is not removed when the entry is replaced") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                require_result_ok(override_result);
                CHECK(*override_result != *emplace_result);

                auto getattr_result = cache.getattr(*emplace_result);
                check_result_ok(getattr_result);
            }

            THEN("The inode has the invalid inode as parent") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                check_result_ok(override_result);

                auto parent_result = cache.parent(*emplace_result);
                require_result_ok(parent_result);
                CHECK(*parent_result == Dragonstash::INVALID_INO);
            }

            THEN("The inode has no name anymore") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                check_result_ok(override_result);

                auto name_result = cache.name(*emplace_result);
                require_result_ok(name_result);
                CHECK(name_result->empty());
            }
        }

        WHEN("An inode is locked in the same transaction as its removal") {
            auto txn = cache.begin_rw();
            auto lock_result = txn.lock(*emplace_result);
            check_result_ok(lock_result);

            THEN("The inode is not removed") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = txn.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                require_result_ok(override_result);
                CHECK(*override_result != *emplace_result);

                check_result_ok(txn.commit());

                auto getattr_result = cache.getattr(*emplace_result);
                check_result_ok(getattr_result);
            }
        }

        WHEN("A lock is rolled back") {
            auto txn = cache.begin_rw();
            auto lock_result = txn.lock(*emplace_result);
            check_result_ok(lock_result);
            txn.abort();

            THEN("The inode is removed when it is replaced") {
                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                require_result_ok(override_result);
                CHECK(*override_result != *emplace_result);

                auto getattr_result = cache.getattr(*emplace_result);
                check_result_error(getattr_result, ENOENT);
            }
        }

        WHEN("A removed, locked inode is unlocked") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            require_result_ok(override_result);
            CHECK(*override_result != *emplace_result);

            auto release_result = cache.release(*emplace_result);
            require_result_ok(release_result);

            THEN("The inode should be removed") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_error(getattr_result, ENOENT);
            }
        }

        WHEN("A removed, locked inode is unlocked and the transaction is aborted") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            require_result_ok(override_result);
            CHECK(*override_result != *emplace_result);

            auto txn = cache.begin_rw();
            auto release_result = txn.release(*emplace_result);
            require_result_ok(release_result);
            txn.abort();

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_ok(getattr_result);
            }
        }

        WHEN("A locked inode is unlocked but not removed") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            auto release_result = cache.release(*emplace_result);
            require_result_ok(release_result);

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_ok(getattr_result);
            }
        }

        WHEN("A locked inode is unlocked and then replaced") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            auto release_result = cache.release(*emplace_result);
            require_result_ok(release_result);

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            require_result_ok(override_result);
            CHECK(*override_result != *emplace_result);

            THEN("The inode should be removed") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_error(getattr_result, ENOENT);
            }
        }

        WHEN("A locked inode is unlocked, but rolled back, and then replaced") {
            auto lock_result = cache.lock(*emplace_result);
            require_result_ok(lock_result);

            auto txn = cache.begin_rw();
            auto release_result = txn.release(*emplace_result);
            require_result_ok(release_result);
            txn.abort();

            Dragonstash::InodeAttributes attr2{
                .mode = S_IFREG
            };
            auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
            require_result_ok(override_result);
            CHECK(*override_result != *emplace_result);

            THEN("The inode should still exist") {
                auto getattr_result = cache.getattr(*emplace_result);
                check_result_ok(getattr_result);
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
                require_result_ok(emplace_result);
                locked_inode = *emplace_result;

                auto lock_result = cache.lock(*emplace_result);
                require_result_ok(lock_result);

                Dragonstash::InodeAttributes attr2{
                    .mode = S_IFREG
                };
                auto override_result = cache.emplace(Dragonstash::ROOT_INO, "entry", attr2);
                require_result_ok(override_result);
                CHECK(*override_result != *emplace_result);
            }

            THEN("The inode is gone after the cache has been restored") {
                Dragonstash::Cache cache(env.path());
                auto getattr_result = cache.getattr(locked_inode);
                check_result_error(getattr_result, ENOENT);
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
                check_result_error(read_result, ENOENT);
            }
        }

        WHEN("Calling writelink on a nonexistent inode") {
            auto write_result = cache.writelink(nonexistent_inode, "/foo");

            THEN("It returns -ENOENT") {
                check_result_error(write_result, ENOENT);
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

        require_result_ok(dir_result);
        require_result_ok(lnk_result);

        WHEN("Writing a destination to the link") {
            std::string destination = "/foo";
            auto write_result = cache.writelink(*lnk_result, destination);

            THEN("It succeeds") {
                check_result_ok(write_result);
            }

            AND_WHEN("Calling getattr") {
                auto getattr_result = cache.getattr(*lnk_result);
                require_result_ok(getattr_result);

                THEN("It returns the length of the link as size") {
                    CHECK(getattr_result->attr.common.size == destination.size());
                }
            }
        }

        WHEN("Calling writelink on a directory") {
            auto write_result = cache.writelink(*dir_result, "/foo");

            THEN("It fails with -EINVAL") {
                check_result_error(write_result, EINVAL);
            }
        }

        WHEN("Calling readlink on a directory") {
            auto read_result = cache.readlink(*dir_result);

            THEN("It fails with -EINVAL") {
                check_result_error(read_result, EINVAL);
            }
        }

        WHEN("Having written a destination on a link") {
            auto write_result = cache.writelink(*lnk_result, "/foo");
            require_result_ok(write_result);

            THEN("It can be read back") {
                auto read_result = cache.readlink(*lnk_result);
                require_result_ok(read_result);
                CHECK(*read_result == "/foo");
            }
        }

        WHEN("Replacing a link with a file") {
            auto write_result = cache.writelink(*lnk_result, "/foo");
            require_result_ok(write_result);

            Dragonstash::InodeAttributes file_attrs = {
                .mode = S_IFREG,
            };

            auto emplace_result = cache.emplace(Dragonstash::ROOT_INO,
                                                "lnk", file_attrs);
            require_result_ok(emplace_result);
            CHECK(*emplace_result != *lnk_result);

            THEN("The link is not readable anymore") {
                auto read_result = cache.readlink(*lnk_result);
                check_result_error(read_result, ENOENT);
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
        require_result_ok(dir1_result);

        WHEN("Emplacing a directory inside the first one") {
            auto dir2_result = cache.emplace(*dir1_result,
                                             "subdir", dir1_attrs);
            require_result_ok(dir2_result);

            THEN("It is not visible in the root directory") {
                auto lookup_result = cache.lookup(Dragonstash::ROOT_INO,
                                                  "subdir");
                check_result_error(lookup_result, ENOENT);
            }

            THEN("It is visible in the first directory") {
                auto lookup_result = cache.lookup(*dir1_result,
                                                  "subdir");
                require_result_ok(lookup_result);
                CHECK(*lookup_result == *dir2_result);
            }
        }

        WHEN("Emplacing a file over a directory with a child") {
            auto dir2_result = cache.emplace(*dir1_result,
                                             "subdir", dir1_attrs);
            require_result_ok(dir2_result);

            Dragonstash::InodeAttributes file_attrs{
                .mode = S_IFREG,
            };

            auto emplace_result = cache.emplace(Dragonstash::ROOT_INO,
                                                "dir", file_attrs);
            require_result_ok(emplace_result);

            THEN("The subdirectory is gone") {
                auto lookup_result = cache.lookup(*dir1_result, "subdir");
                check_result_error(lookup_result, ENOENT);

                auto getattr_result = cache.getattr(*dir2_result);
                check_result_error(getattr_result, ENOENT);
            }
        }
    }
}

SCENARIO("Reading directories") {
    GIVEN("An empty cache") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        WHEN("Iterating readdir on the root inode") {
            auto txn = cache.begin_ro();

            THEN("Only dot is returned. I am sorry.") {
                Dragonstash::Result<Dragonstash::DirectoryEntry> result = make_result(Dragonstash::FAILED, 0);

                result = txn.readdir(Dragonstash::ROOT_INO, 0);
                require_result_ok(result);
                CHECK(result->name == ".");
                CHECK(result->ino == Dragonstash::ROOT_INO);

                result = txn.readdir(Dragonstash::ROOT_INO, result->ino);
                check_result_error(result, 0);
            }
        }
    }

    GIVEN("A cache with a directory") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        Dragonstash::InodeAttributes parent_attrs{
            .mode = S_IFDIR,
        };

        auto parent_result = cache.emplace(Dragonstash::ROOT_INO,
                                           "dir", parent_attrs);
        require_result_ok(parent_result);

        WHEN("Iterating readdir on that directory") {
            auto txn = cache.begin_ro();

            THEN("Dot and dotdot are returned") {
                Dragonstash::Result<Dragonstash::DirectoryEntry> result = make_result(Dragonstash::FAILED, 0);

                result = txn.readdir(*parent_result, 0);
                require_result_ok(result);
                CHECK(result->name == ".");
                CHECK(result->ino == *parent_result);

                result = txn.readdir(*parent_result, result->ino);
                require_result_ok(result);
                CHECK(result->name == "..");
                CHECK(result->ino == Dragonstash::ROOT_INO);

                result = txn.readdir(*parent_result, result->ino);
                check_result_error(result, 0);
            }
        }
    }

    GIVEN("A cache with a subdirectory with a few directory entries") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();
        Dragonstash::InodeAttributes parent_attrs{
            .mode = S_IFDIR,
        };

        Dragonstash::InodeAttributes f1_attrs{
            .mode = S_IFDIR,
        };
        Dragonstash::InodeAttributes f2_attrs{
            .mode = S_IFREG,
        };
        Dragonstash::InodeAttributes f3_attrs{
            .mode = S_IFLNK,
        };

        auto parent_result = cache.emplace(Dragonstash::ROOT_INO,
                                           "dir", parent_attrs);
        require_result_ok(parent_result);

        auto ent1_result = cache.emplace(*parent_result,
                                         "f1", f1_attrs);
        require_result_ok(ent1_result);

        auto ent2_result = cache.emplace(*parent_result,
                                         "f2", f2_attrs);
        require_result_ok(ent2_result);

        auto ent3_result = cache.emplace(*parent_result,
                                         "f3", f3_attrs);
        require_result_ok(ent3_result);

        WHEN("Using readdir initially") {
            auto txn = cache.begin_ro();
            auto readdir_result = txn.readdir(*parent_result, 0);

            THEN("It returns dot") {
                require_result_ok(readdir_result);
                CHECK(readdir_result->name == ".");
                CHECK(!readdir_result->complete);
                CHECK(readdir_result->ino == *parent_result);
            }
        }

        WHEN("Using readdir iteratively") {
            auto txn = cache.begin_ro();
            THEN("All entries should be returned, and then EOF") {
                Dragonstash::Result<Dragonstash::DirectoryEntry> result = make_result(Dragonstash::FAILED, 0);

                result = txn.readdir(*parent_result, 0);
                require_result_ok(result);
                CHECK(result->name == ".");
                CHECK(result->ino == *parent_result);

                result = txn.readdir(*parent_result, result->ino);
                require_result_ok(result);
                CHECK(result->name == "..");
                CHECK(result->ino == Dragonstash::ROOT_INO);

                result = txn.readdir(*parent_result, result->ino);
                REQUIRE(result);
                CHECK(result->name == "f1");
                CHECK(result->ino == *ent1_result);

                result = txn.readdir(*parent_result, result->ino);
                REQUIRE(result);
                CHECK(result->name == "f2");
                CHECK(result->ino == *ent2_result);

                result = txn.readdir(*parent_result, result->ino);
                REQUIRE(result);
                CHECK(result->name == "f3");
                CHECK(result->ino == *ent3_result);

                result = txn.readdir(*parent_result, result->ino);
                check_result_error(result, 0);
            }
        }
    }
}

SCENARIO("Path reconstruction") {
    GIVEN("A cache with a nested file and directory structure") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        Dragonstash::InodeAttributes dir_attrs{
            .mode = S_IFDIR,
        };
        Dragonstash::InodeAttributes file_attrs{
            .mode = S_IFREG,
        };

        auto d1_r = cache.emplace(Dragonstash::ROOT_INO, "d1", dir_attrs);
        require_result_ok(d1_r);

        auto d2_r = cache.emplace(Dragonstash::ROOT_INO, "d2", dir_attrs);
        require_result_ok(d2_r);

        auto f1_r = cache.emplace(Dragonstash::ROOT_INO, "f1", file_attrs);
        require_result_ok(f1_r);

        auto d1_f1_r = cache.emplace(*d1_r, "f1", file_attrs);
        require_result_ok(d1_f1_r);

        auto d1_d1_r = cache.emplace(*d1_r, "d1", dir_attrs);
        require_result_ok(d1_d1_r);

        auto d1_d1_f1_r = cache.emplace(*d1_d1_r, "f1", file_attrs);
        require_result_ok(d1_d1_f1_r);

        auto d2_f1_r = cache.emplace(*d2_r, "f1", file_attrs);
        require_result_ok(d2_f1_r);

        WHEN("Reconstructing path of the root inode") {
            auto path = cache.path(Dragonstash::ROOT_INO);

            THEN("It should succeed and be empty") {
                require_result_ok(path);
                CHECK(path->empty());
            }
        }

        WHEN("Reconstructing path of /d1") {
            auto path = cache.path(*d1_r);

            THEN("It should succeed and be correct") {
                require_result_ok(path);
                CHECK(*path == "/d1");
            }
        }

        WHEN("Reconstructing path of /f1") {
            auto path = cache.path(*f1_r);

            THEN("It should succeed and be correct") {
                require_result_ok(path);
                CHECK(*path == "/f1");
            }
        }

        WHEN("Reconstructing path of /d2/f1") {
            auto path = cache.path(*d2_f1_r);

            THEN("It should succeed and be correct") {
                require_result_ok(path);
                CHECK(*path == "/d2/f1");
            }
        }

        WHEN("Reconstructing path of /d1/d1/f1") {
            auto path = cache.path(*d1_d1_f1_r);

            THEN("It should succeed and be correct") {
                require_result_ok(path);
                CHECK(*path == "/d1/d1/f1");
            }
        }
    }
}

SCENARIO("Single-thread Concurrency") {
    GIVEN("A cache with a few entries") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        Dragonstash::InodeAttributes dir_attrs{
            .mode = S_IFDIR,
        };
        Dragonstash::InodeAttributes file_attrs{
            .mode = S_IFREG,
        };

        auto d1_r = cache.emplace(Dragonstash::ROOT_INO, "d1", dir_attrs);
        require_result_ok(d1_r);

        auto d2_r = cache.emplace(Dragonstash::ROOT_INO, "d2", dir_attrs);
        require_result_ok(d2_r);

        auto f1_r = cache.emplace(Dragonstash::ROOT_INO, "f1", file_attrs);
        require_result_ok(f1_r);

        WHEN("Attempting to use multiple readers concurrently") {
            auto r1 = cache.begin_ro();
            auto r2 = cache.begin_ro();

            THEN("It works") {
                check_result_ok(r1.lookup(Dragonstash::ROOT_INO, "d1"));
                check_result_ok(r2.lookup(Dragonstash::ROOT_INO, "d2"));
            }
        }

        WHEN("Attempting to use a reader and a writer concurrently") {
            auto r1 = cache.begin_ro();
            auto w1 = cache.begin_rw();

            THEN("It works") {
                check_result_ok(r1.lookup(Dragonstash::ROOT_INO, "d1"));
                check_result_ok(w1.lookup(Dragonstash::ROOT_INO, "d2"));
            }
        }

        WHEN("Using a reader and writer concurrently") {
            auto r1 = cache.begin_ro();
            auto w1 = cache.begin_rw();

            THEN("The reader is unaffected by the writer") {
                auto emplace_result = w1.emplace(*d1_r, "foo", file_attrs);
                check_result_ok(emplace_result);

                auto lookup_result = r1.lookup(*d1_r, "foo");
                check_result_error(lookup_result, ENOENT);
            }

            THEN("The reader does not see the writer's results after commit") {
                auto emplace_result = w1.emplace(*d1_r, "foo", file_attrs);
                check_result_ok(emplace_result);
                check_result_ok(w1.commit());

                auto lookup_result = r1.lookup(*d1_r, "foo");
                check_result_error(lookup_result, ENOENT);
            }

            THEN("A new reader does see the writer's results after commit") {
                auto emplace_result = w1.emplace(*d1_r, "foo", file_attrs);
                check_result_ok(emplace_result);
                check_result_ok(w1.commit());

                auto lookup_result = r1.lookup(*d1_r, "foo");
                check_result_error(lookup_result, ENOENT);

                auto r2 = cache.begin_ro();
                lookup_result = r2.lookup(*d1_r, "foo");
                require_result_ok(lookup_result);
                CHECK(*lookup_result == *emplace_result);

                lookup_result = r1.lookup(*d1_r, "foo");
                check_result_error(lookup_result, ENOENT);
            }
        }
    }
}

SCENARIO("Read-committed isolation level for locks") {
    GIVEN("A cache with two locked entries") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        Dragonstash::InodeAttributes dir_attrs{
            .mode = S_IFDIR,
        };

        Dragonstash::InodeAttributes file_attrs{
            .mode = S_IFREG,
        };

        auto d1_r = cache.emplace(Dragonstash::ROOT_INO, "d1", dir_attrs);
        require_result_ok(d1_r);

        auto d2_r = cache.emplace(Dragonstash::ROOT_INO, "d2", dir_attrs);
        require_result_ok(d2_r);

        auto lock_result = cache.lock(*d1_r);
        require_result_ok(lock_result);

        lock_result = cache.lock(*d2_r);
        require_result_ok(lock_result);

        WHEN("A writer overrides the inode, releases the lock and the reader looks up the inode before commit") {
            auto r1 = cache.begin_ro();
            auto w1 = cache.begin_rw();

            auto release_result = w1.release(*d2_r, 1);
            check_result_ok(release_result);

            auto emplace_result = w1.emplace(Dragonstash::ROOT_INO, "d2", file_attrs);
            require_result_ok(emplace_result);

            auto w_lookup_result = w1.lookup(Dragonstash::ROOT_INO, "d2");
            require_result_ok(w_lookup_result);
            CHECK(*w_lookup_result == *emplace_result);

            auto r_lookup_result = r1.lookup(Dragonstash::ROOT_INO, "d1");
            require_result_ok(r_lookup_result);

            THEN("The lookup still shows the old inode") {
                CHECK(*r_lookup_result == *d1_r);
            }

            THEN("An attempt to lock the inode fails with a deadlock") {
                if (Dragonstash::Cache::deadlock_detection) {
                    // the behaviour of a normal mutex is undefined when a
                    // thread already owns the mutex
                    // the debug_mutex supports an additional out-of-band check
                    // for mutex ownership, but only if compiled without NDEBUG
                    // The deadlock_detection flag indicates if the mutex used
                    // by libdragonstash was compiled without NDEBUG.
                    CHECK_THROWS_AS(r1.lock(*r_lookup_result), std::system_error);
                }
            }
        }

        WHEN("A writer overrides the inode, releases the lock and the reader attempts to lock the inode after commit") {
            auto r1 = cache.begin_ro();
            auto w1 = cache.begin_rw();

            auto release_result = w1.release(*d2_r, 1);
            check_result_ok(release_result);

            auto emplace_result = w1.emplace(Dragonstash::ROOT_INO, "d2", file_attrs);
            require_result_ok(emplace_result);
            check_result_ok(w1.clean_orphans());

            auto w_lookup_result = w1.lookup(Dragonstash::ROOT_INO, "d2");
            require_result_ok(w_lookup_result);
            CHECK(*w_lookup_result == *emplace_result);

            auto w_getattr_result = w1.getattr(*d2_r);
            check_result_error(w_getattr_result, ENOENT);

            auto r_lookup_result = r1.lookup(Dragonstash::ROOT_INO, "d2");
            require_result_ok(r_lookup_result);
            require_result_ok(w1.commit());

            THEN("The lock fails with ESTALE") {
                CHECK(*r_lookup_result == *d2_r);

                auto lock_result = r1.lock(*r_lookup_result);
                check_result_error(lock_result, ESTALE);
            }
        }
    }
}

SCENARIO("Transaction hooks") {
    GIVEN("An empty cache") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        WHEN("Adding a passing commit hook to a tranasction") {
            auto txn = cache.begin_rw();
            std::atomic<bool> stage_1_ran(false);
            std::atomic<bool> stage_1_rollback_ran(false);
            std::atomic<bool> stage_2_ran(false);
            txn.add_commit_hook([&stage_1_ran](){
                stage_1_ran = true;
                return Dragonstash::make_result();
            }, [&stage_1_rollback_ran](){
                stage_1_rollback_ran = true;
            }, [&stage_2_ran](){
                stage_2_ran = true;
            });
            CHECK(!stage_1_ran);
            CHECK(!stage_1_rollback_ran);
            CHECK(!stage_2_ran);

            AND_WHEN("The transaction commits") {
                check_result_ok(txn.commit());

                THEN("Both stages are called") {
                    CHECK(stage_1_ran);
                    CHECK(stage_2_ran);
                }

                THEN("Stage 1 rollback is not called") {
                    CHECK(!stage_1_rollback_ran);
                }
            }

            AND_WHEN("The transaction aborts") {
                txn.abort();

                THEN("Neither stage is called") {
                    CHECK(!stage_1_ran);
                    CHECK(!stage_2_ran);
                }

                THEN("Stage 1 rollback is not called") {
                    CHECK(!stage_1_rollback_ran);
                }
            }
        }

        WHEN("Adding a failing commit hook to a tranasction") {
            static constexpr int random_errno = 1234;
            auto txn = cache.begin_rw();

            Dragonstash::InodeAttributes dir_attrs{
                .mode = S_IFDIR,
            };
            auto emplace_result = txn.emplace(Dragonstash::ROOT_INO, "test", dir_attrs);

            std::atomic<bool> stage_1_ran(false);
            std::atomic<bool> stage_1_rollback_ran(false);
            std::atomic<bool> stage_2_ran(false);
            std::atomic<bool> rollback_ran(false);
            txn.add_transaction_hook([&stage_1_ran](){
                stage_1_ran = true;
                return Dragonstash::make_result(Dragonstash::FAILED, random_errno);
            }, [&stage_1_rollback_ran](){
                stage_1_rollback_ran = true;
            }, [&stage_2_ran](){
                stage_2_ran = true;
            }, [&rollback_ran](){
                rollback_ran = true;
            });
            CHECK(!stage_1_ran);
            CHECK(!stage_1_rollback_ran);
            CHECK(!stage_2_ran);
            CHECK(!rollback_ran);

            AND_WHEN("The transaction attempts to commit") {
                auto commit_result = txn.commit();

                THEN("The commit fails with the given error") {
                    check_result_error(commit_result, random_errno);
                }

                THEN("Stage 1 is called") {
                    CHECK(stage_1_ran);
                }

                THEN("Stage 2 is not called") {
                    CHECK(!stage_2_ran);
                }

                THEN("Stage 1 rollback is not called") {
                    CHECK(!stage_1_rollback_ran);
                }

                THEN("Changes made by the transaction are not visible") {
                    check_result_ok(emplace_result);

                    auto lookup_result = cache.lookup(Dragonstash::ROOT_INO, "test");
                    check_result_error(lookup_result, ENOENT);
                }

                THEN("The transaction has stopped") {
                    auto other_txn = cache.begin_rw();
                    CHECK(other_txn);
                }

                THEN("Rollback is called") {
                    CHECK(rollback_ran);
                }
            }

            AND_WHEN("The transaction aborts") {
                txn.abort();

                THEN("No handler is called") {
                    CHECK(!stage_1_ran);
                    CHECK(!stage_2_ran);
                    CHECK(!stage_1_rollback_ran);
                }

                THEN("Changes made by the transaction are not visible") {
                    check_result_ok(emplace_result);

                    auto lookup_result = cache.lookup(Dragonstash::ROOT_INO, "test");
                    check_result_error(lookup_result, ENOENT);
                }

                THEN("Rollback is called") {
                    CHECK(rollback_ran);
                }
            }
        }

        WHEN("Adding three commit hooks: c1 (passing), c2 (failing), c3 (passing)") {
            auto txn = cache.begin_rw();
            std::atomic<unsigned int> ctr(0);
            std::atomic<unsigned int> c1_stage_1_ran(0);
            std::atomic<unsigned int> c1_stage_1_rollback_ran(0);
            std::atomic<unsigned int> c1_stage_2_ran(0);
            std::atomic<unsigned int> c1_rollback_ran(0);
            std::atomic<unsigned int> c2_stage_1_ran(0);
            std::atomic<unsigned int> c2_stage_1_rollback_ran(0);
            std::atomic<unsigned int> c2_stage_2_ran(0);
            std::atomic<unsigned int> c2_rollback_ran(0);
            std::atomic<unsigned int> c3_stage_1_ran(0);
            std::atomic<unsigned int> c3_stage_1_rollback_ran(0);
            std::atomic<unsigned int> c3_stage_2_ran(0);
            std::atomic<unsigned int> c3_rollback_ran(0);

            txn.add_transaction_hook([&c1_stage_1_ran, &ctr](){
                c1_stage_1_ran = ++ctr;
                return Dragonstash::make_result();
            }, [&c1_stage_1_rollback_ran, &ctr](){
                c1_stage_1_rollback_ran = ++ctr;
            }, [&c1_stage_2_ran, &ctr](){
                c1_stage_2_ran = ++ctr;
            }, [&c1_rollback_ran, &ctr](){
                c1_rollback_ran = ++ctr;
            });

            txn.add_transaction_hook([&c2_stage_1_ran, &ctr](){
                c2_stage_1_ran = ++ctr;
                return Dragonstash::make_result(Dragonstash::FAILED, EINVAL);
            }, [&c2_stage_1_rollback_ran, &ctr](){
                c2_stage_1_rollback_ran = ++ctr;
            }, [&c2_stage_2_ran, &ctr](){
                c2_stage_2_ran = ++ctr;
            }, [&c2_rollback_ran, &ctr](){
                c2_rollback_ran = ++ctr;
            });

            txn.add_transaction_hook([&c3_stage_1_ran, &ctr](){
                c3_stage_1_ran = ++ctr;
                return Dragonstash::make_result();
            }, [&c3_stage_1_rollback_ran, &ctr](){
                c3_stage_1_rollback_ran = ++ctr;
            }, [&c3_stage_2_ran, &ctr](){
                c3_stage_2_ran = ++ctr;
            }, [&c3_rollback_ran, &ctr](){
                c3_rollback_ran = ++ctr;
            });

            AND_WHEN("The transaction attempts to commit") {
                auto commit_result = txn.commit();

                THEN("Commit fails") {
                    check_result_error(commit_result, EINVAL);
                }

                THEN("Stage 1 runs for c1 and c2 in the correct order") {
                    CHECK(c1_stage_1_ran);
                    CHECK(c2_stage_1_ran);
                }

                THEN("Stage 1 does not run for c3") {
                    CHECK(!c3_stage_1_ran);
                }

                THEN("Stage 1 rollback runs for c1, but not for c2 or c3") {
                    CHECK(c1_stage_1_rollback_ran);
                    CHECK(!c2_stage_1_rollback_ran);
                    CHECK(!c3_stage_1_rollback_ran);
                }

                THEN("Stage 2 is not run for any") {
                    CHECK(!c1_stage_2_ran);
                    CHECK(!c2_stage_2_ran);
                    CHECK(!c3_stage_2_ran);
                }

                THEN("Rollback runs for all") {
                    CHECK(c1_rollback_ran);
                    CHECK(c2_rollback_ran);
                    CHECK(c3_rollback_ran);
                }

                THEN("All hooks are called in the right order") {
                    CHECK(c1_stage_1_ran < c2_stage_1_ran);
                    CHECK(c2_stage_2_ran < c1_stage_1_rollback_ran);
                    CHECK(c1_stage_1_rollback_ran < c3_rollback_ran);
                    CHECK(c3_rollback_ran < c2_rollback_ran);
                    CHECK(c2_rollback_ran < c1_rollback_ran);
                }
            }
        }
    }
}

SCENARIO("Transaction nesting") {
    GIVEN("A cache with a directory") {
        TestSetup setup;
        Dragonstash::Cache &cache = setup.cache();

        Dragonstash::InodeAttributes dir_attrs{
            .mode = S_IFDIR,
        };

        Dragonstash::InodeAttributes file_attrs{
            .mode = S_IFREG,
        };

        auto d1_r = cache.emplace(Dragonstash::ROOT_INO, "d1", dir_attrs);
        require_result_ok(d1_r);

        WHEN("Locking an inode in both the parent and the nested transaction") {
            auto txn = cache.begin_rw();
            auto lock1_result = txn.lock(*d1_r);
            auto txn2 = txn.begin_nested();
            auto lock2_result = txn2.lock(*d1_r);

            THEN("Both locks succeed") {
                check_result_ok(lock1_result);
                check_result_ok(lock2_result);
            }

            THEN("The file can be released twice from the inner transaction") {
                check_result_ok(txn2.release(*d1_r, 2));
            }

            THEN("The file can be released only once after the inner transaction has been aborted") {
                txn2.abort();
                CHECK_THROWS_WITH(
                            txn.release(*d1_r, 2),
                            Catch::Matchers::Contains("counter below zero"));
                check_result_ok(txn.release(*d1_r, 1));
            }

            THEN("The file can be released twice after the inner transaction has committed") {
                check_result_ok(txn2.commit());
                check_result_ok(txn.release(*d1_r, 2));
            }
        }

        WHEN("Adding a rollback hook to a nested transaction") {
            auto txn = cache.begin_rw();
            auto txn2 = txn.begin_nested();
            std::atomic<bool> signal(false);
            txn2.add_rollback_hook([&signal](){
                signal = true;
            });
            CHECK(!signal);

            AND_WHEN("Aborting the nested transaction") {
                txn2.abort();
                THEN("The handler is executed") {
                    CHECK(signal);
                }

                AND_WHEN("The parent transaction aborts") {
                    signal = false;
                    txn.abort();
                    THEN("The handler is not executed again") {
                        CHECK(!signal);
                    }
                }
            }

            AND_WHEN("Comitting the nested transaction") {
                check_result_ok(txn2.commit());

                THEN("The handler is not executed") {
                    CHECK(!signal);
                }

                AND_WHEN("Comitting the parent transaction") {
                    check_result_ok(txn.commit());

                    THEN("The handler is not executed") {
                        CHECK(!signal);
                    }
                }

                AND_WHEN("Aborting the parent transaction") {
                    txn.abort();

                    THEN("The handler is executed") {
                        CHECK(signal);
                    }
                }
            }
        }

        WHEN("Adding a commit handler to a nested transaction") {
            auto txn = cache.begin_rw();
            auto txn2 = txn.begin_nested();
            std::atomic<bool> stage_1_ran(false);
            std::atomic<bool> stage_2_ran(false);
            txn2.add_commit_hook([&stage_1_ran](){
                stage_1_ran = true;
                return Dragonstash::make_result();
            }, nullptr, [&stage_2_ran](){
                stage_2_ran = true;
            });
            CHECK(!stage_1_ran);
            CHECK(!stage_2_ran);

            AND_WHEN("Aborting the nested transaction") {
                txn2.abort();

                THEN("Neither stage is called") {
                    CHECK(!stage_1_ran);
                    CHECK(!stage_2_ran);
                }

                AND_WHEN("Committing the parent transaction") {
                    check_result_ok(txn.commit());

                    THEN("Neither stage is called") {
                        CHECK(!stage_1_ran);
                        CHECK(!stage_2_ran);
                    }
                }

                AND_WHEN("Aborting the parent transaction") {
                    txn.abort();

                    THEN("Neither stage is called") {
                        CHECK(!stage_1_ran);
                        CHECK(!stage_2_ran);
                    }
                }
            }

            AND_WHEN("Committing the nested transaction") {
                check_result_ok(txn2.commit());

                THEN("Neither stage is called") {
                    CHECK(!stage_1_ran);
                    CHECK(!stage_2_ran);
                }

                AND_WHEN("Committing the parent transaction") {
                    check_result_ok(txn.commit());

                    THEN("Both stages are called") {
                        CHECK(stage_1_ran);
                        CHECK(stage_2_ran);
                    }
                }

                AND_WHEN("Aborting the parent transaction") {
                    txn.abort();

                    THEN("Neither stage is called") {
                        CHECK(!stage_1_ran);
                        CHECK(!stage_2_ran);
                    }
                }
            }
        }
    }
}

SCENARIO("Inode flags")
{
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();

    GIVEN("An empty cache") {
        WHEN("Testing the root inode for a flag") {
            auto txn = cache.begin_ro();
            auto result = txn.test_flag(Dragonstash::ROOT_INO,
                                        Dragonstash::InodeFlag::SYNCED);

            THEN("It succeeds and returns false") {
                require_result_ok(result);
                CHECK(!*result);
            }
        }

        WHEN("Testing a nonexistent inode for a flag") {
            auto txn = cache.begin_ro();
            auto result = txn.test_flag(nonexistent_inode,
                                        Dragonstash::InodeFlag::SYNCED);

            THEN("It fails with ENOENT") {
                check_result_error(result, ENOENT);
            }
        }

        WHEN("Setting a flag on a nonexistent inode") {
            auto txn = cache.begin_rw();
            auto result = txn.update_flags(nonexistent_inode,
                                           {Dragonstash::InodeFlag::SYNCED});

            THEN("It fails with ENOENT") {
                check_result_error(result, ENOENT);
            }
        }

        WHEN("Setting a flag on the root") {
            Dragonstash::Result<void> result;
            {
                auto txn = cache.begin_rw();
                result = txn.update_flags(Dragonstash::ROOT_INO,
                                          {Dragonstash::InodeFlag::SYNCED});
                check_result_ok(txn.commit());
            }

            THEN("It succeeds") {
                check_result_ok(result);
            }

            AND_WHEN("Testing that flag") {
                auto txn = cache.begin_ro();
                auto result = txn.test_flag(Dragonstash::ROOT_INO,
                                            Dragonstash::InodeFlag::SYNCED);

                THEN("It succeeds and returns true") {
                    require_result_ok(result);
                    CHECK(*result);
                }
            }

            AND_WHEN("Clearing that flag on the root") {
                Dragonstash::Result<void> result;
                {
                    auto txn = cache.begin_rw();
                    result = txn.update_flags(Dragonstash::ROOT_INO,
                                              {},
                                              {Dragonstash::InodeFlag::SYNCED});
                    check_result_ok(txn.commit());
                }

                THEN("It succeds") {
                    check_result_ok(result);
                }

                AND_WHEN("Testing that flag") {
                    auto txn = cache.begin_ro();
                    auto result = txn.test_flag(Dragonstash::ROOT_INO,
                                                Dragonstash::InodeFlag::SYNCED);

                    THEN("It succeds and returns false") {
                        require_result_ok(result);
                        CHECK(!*result);
                    }
                }
            }
        }
    }
}

SCENARIO("Directory rewriting") {
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();
    Dragonstash::InodeAttributes dir_attr{
        .mode = S_IFDIR
    };
    Dragonstash::InodeAttributes reg_attr{
        .mode = S_IFREG
    };

    GIVEN("A cache with a few entries in the root") {
        const Dragonstash::ino_t dir_ino = Dragonstash::ROOT_INO;
        std::vector<ino_t> entry_inos;
        {
            auto txn = cache.begin_rw();
            auto add_entry = [&entry_inos, &cache, dir_ino, &txn](std::string_view name, const Dragonstash::InodeAttributes &attr) {
                auto emplace_result = txn.emplace(dir_ino, name, attr);
                require_result_ok(emplace_result);
                entry_inos.emplace_back(*emplace_result);
            };
            add_entry("e1", dir_attr);
            add_entry("e2", reg_attr);
            add_entry("e3", reg_attr);
            check_result_ok(txn.commit());
        }

        WHEN("Rewriting the directory without re-emplacing entries") {
            {
                auto txn = cache.begin_rw();
                auto start_result = txn.start_dir_rewrite(dir_ino);
                check_result_ok(start_result);

                auto end_result = txn.finish_dir_rewrite();
                check_result_ok(end_result);
                check_result_ok(txn.clean_orphans());
                check_result_ok(txn.commit());
            }

            THEN("The entries are gone") {
                for (ino_t entry_ino: entry_inos) {
                    auto getattr_result = cache.getattr(entry_ino);
                    check_result_error(getattr_result, ENOENT);
                }
            }

            THEN("The entries do not show up in readdir") {
                auto txn = cache.begin_ro();
                auto readdir_result = txn.readdir(dir_ino, Dragonstash::INVALID_INO);
                require_result_ok(readdir_result);
                CHECK(readdir_result->name == ".");

                readdir_result = txn.readdir(dir_ino, readdir_result->ino);
                check_result_error(readdir_result, 0);
            }
        }

        WHEN("Rewriting the directory with re-emplacing some entries") {
            {
                auto txn = cache.begin_rw();
                auto start_result = txn.start_dir_rewrite(dir_ino);
                check_result_ok(start_result);

                auto emplace_result = txn.emplace(dir_ino, "e1", dir_attr);
                require_result_ok(emplace_result);
                CHECK(*emplace_result == entry_inos[0]);

                emplace_result = txn.emplace(dir_ino, "e3", reg_attr);
                require_result_ok(emplace_result);
                CHECK(*emplace_result == entry_inos[2]);

                check_result_ok(txn.finish_dir_rewrite());
                check_result_ok(txn.clean_orphans());
                check_result_ok(txn.commit());
            }

            THEN("The re-emplaced entries are there") {
                {
                    auto getattr_result = cache.getattr(entry_inos[0]);
                    check_result_ok(getattr_result);
                }

                {
                    auto getattr_result = cache.getattr(entry_inos[2]);
                    check_result_ok(getattr_result);
                }
            }

            THEN("The untouched entry is gone") {
                auto getattr_result = cache.getattr(entry_inos[1]);
                check_result_error(getattr_result, ENOENT);
            }
        }
    }
}

SCENARIO("Unlinking") {
    TestSetup setup;
    Dragonstash::Cache &cache = setup.cache();
    Dragonstash::InodeAttributes dir_attr{
        .mode = S_IFDIR
    };
    Dragonstash::InodeAttributes reg_attr{
        .mode = S_IFREG
    };

    GIVEN("An empty cache") {
        WHEN("Calling unlink on a nonexistent inode") {
            auto txn = cache.begin_rw();
            auto unlink_result = txn.unlink(nonexistent_inode);

            THEN("It fails with ENOENT") {
                check_result_error(unlink_result, ENOENT);
            }
        }

        WHEN("Calling unlink on a nonexistent entry") {
            auto txn = cache.begin_rw();
            auto unlink_result = txn.unlink(Dragonstash::ROOT_INO, "ex");

            THEN("It fails with ENOENT") {
                check_result_error(unlink_result, ENOENT);
            }
        }
    }

    GIVEN("A cache with a few entries in the root") {
        const Dragonstash::ino_t dir_ino = Dragonstash::ROOT_INO;
        std::vector<ino_t> entry_inos;
        {
            auto txn = cache.begin_rw();
            auto add_entry = [&entry_inos, &cache, dir_ino, &txn](std::string_view name, const Dragonstash::InodeAttributes &attr) {
                auto emplace_result = txn.emplace(dir_ino, name, attr);
                require_result_ok(emplace_result);
                entry_inos.emplace_back(*emplace_result);
            };
            add_entry("e1", dir_attr);
            add_entry("e2", reg_attr);
            add_entry("e3", reg_attr);
            check_result_ok(txn.commit());
        }

        WHEN("Calling unlink on a file by its inode") {
            {
                auto txn = cache.begin_rw();
                check_result_ok(txn.unlink(entry_inos[1]));
                check_result_ok(txn.commit());
            }

            THEN("Calling getattr on the file fails with ENOENT") {
                check_result_error(cache.getattr(entry_inos[1]), ENOENT);
            }

            THEN("Calling getattr on the other files succeds") {
                check_result_ok(cache.getattr(entry_inos[0]));
                check_result_ok(cache.getattr(entry_inos[2]));
            }
        }

        WHEN("Calling unlink on a file by its parent + inode") {
            {
                auto txn = cache.begin_rw();
                check_result_ok(txn.unlink(Dragonstash::ROOT_INO, entry_inos[1]));
                check_result_ok(txn.commit());
            }

            THEN("Calling getattr on the file fails with ENOENT") {
                check_result_error(cache.getattr(entry_inos[1]), ENOENT);
            }

            THEN("Calling getattr on the other files succeds") {
                check_result_ok(cache.getattr(entry_inos[0]));
                check_result_ok(cache.getattr(entry_inos[2]));
            }
        }

        WHEN("Calling unlink on a file by its parent + name") {
            {
                auto txn = cache.begin_rw();
                auto result = txn.unlink(Dragonstash::ROOT_INO, "e2");
                check_result_ok(result);
                check_result_ok(txn.commit());
            }

            THEN("Calling getattr on the file fails with ENOENT") {
                check_result_error(cache.getattr(entry_inos[1]), ENOENT);
            }

            THEN("Calling getattr on the other files succeds") {
                check_result_ok(cache.getattr(entry_inos[0]));
                check_result_ok(cache.getattr(entry_inos[2]));
            }
        }
    }
}
