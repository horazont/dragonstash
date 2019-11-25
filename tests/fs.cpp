/**********************************************************************
File name: fs.cpp
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

#include "dragonstash/in_memory_backend.hpp"
#include "dragonstash/cache/cache.hpp"
#include "dragonstash/fs.hpp"

#include "testutils/tempdir.hpp"
#include "testutils/fuse_backend.hpp"
#include "testutils/result.hpp"

class TestEnvironment {
public:
    TestEnvironment():
        m_cache(m_cachedir.path()),
        m_fs(m_cache, m_backend),
        m_default_uid(getuid()),
        m_default_gid(getgid()),
        m_default_timestamp{.tv_sec = 1536390000, .tv_nsec = 20180908}
    {

    }

private:
    TemporaryDirectory m_cachedir;
    Dragonstash::Cache m_cache;
    Dragonstash::Backend::InMemoryFilesystem m_backend;
    Dragonstash::Filesystem m_fs;
    TestFuseBackend m_fuse;

    uid_t m_default_uid;
    gid_t m_default_gid;

    struct timespec m_default_timestamp;

public:
    [[nodiscard]] uid_t default_uid() const {
        return m_default_uid;
    }

    [[nodiscard]] gid_t default_gid() const {
        return m_default_gid;
    }

    [[nodiscard]] const struct timespec &default_timestamp() const {
        return m_default_timestamp;
    }

    [[nodiscard]] inline Dragonstash::Cache &cache() {
        return m_cache;
    }

    [[nodiscard]] inline Dragonstash::Backend::InMemoryFilesystem &backend() {
        return m_backend;
    }

    [[nodiscard]] inline Dragonstash::Filesystem &fs() {
        return m_fs;
    }

    [[nodiscard]] inline TestFuseBackend &fuse() {
        return m_fuse;
    }

    TestEnvironment &with_default_contents() {
        Dragonstash::Backend::Stat file_attr{
            .mode = S_IRUSR | S_IWUSR | S_IRGRP,
            .uid = m_default_uid,
            .gid = m_default_gid,
            .atime = m_default_timestamp,
            .mtime = m_default_timestamp,
            .ctime = m_default_timestamp,
        };

        Dragonstash::Backend::Stat dir_attr{
            .mode = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP,
            .uid = m_default_uid,
            .gid = m_default_gid,
            .atime = m_default_timestamp,
            .mtime = m_default_timestamp,
            .ctime = m_default_timestamp,
        };

        using namespace Dragonstash::Backend::InMemory;
        m_backend.emplace<File>("README.md").update_attr(file_attr);
        auto &dir = m_backend.emplace<Directory>("books");
        dir.update_attr(dir_attr);
        dir.emplace<File>("Hitchhiker's Guide To The Galaxy.epub").update_attr(file_attr);
        dir.emplace<File>("The Elements of Style.epub").update_attr(file_attr);
        dir.emplace<Link>("best.epub", "Hitchhiker's Guide To The Galaxy.epub").update_attr(file_attr);
        return *this;
    }
};

void check_reply_type(const TestFuseRequest &req, TestFuseReplyType expected_type)
{
    REQUIRE(req.has_reply());
    if (req.reply_type() != expected_type) {
        if (req.reply_type() == TestFuseReplyType::ERROR) {
            auto err = std::get<TestFuseReplyErr>(req.reply_argv());
            CHECK(err == 0);
        }
    }
    REQUIRE(req.reply_type() == expected_type);
}

void check_reply_error(const TestFuseRequest &req, int err) {
    REQUIRE(req.has_reply());
    CHECK(req.reply_type() == TestFuseReplyType::ERROR);
    if (req.reply_type() == TestFuseReplyType::ERROR) {
        CHECK(std::get<TestFuseReplyErr>(req.reply_argv()) == err);
    }
}

Dragonstash::Result<ino_t> lookup(TestFuseBackend &fuse,
                                  Dragonstash::Filesystem &fs,
                                  ino_t parent,
                                  std::string_view name)
{
    auto req = fuse.new_request();
    fs.lookup(req.wrap(), parent, name);
    if (req.reply_type() == TestFuseReplyType::ERROR) {
        return Dragonstash::make_result(Dragonstash::FAILED, std::get<TestFuseReplyErr>(req.reply_argv()));
    }

    REQUIRE(req.reply_type() == TestFuseReplyType::ENTRY);
    return std::get<TestFuseReplyEntry>(req.reply_argv()).ino;
}

SCENARIO("lookup") {
    TestEnvironment env;

    GIVEN("A filesystem with default contents") {
        env.with_default_contents();
        Dragonstash::Filesystem &fs = env.fs();

        WHEN("Requesting to look up an existing file") {
            auto req = env.fuse().new_request();
            fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");

            THEN("The FS replies with an entry") {
                check_reply_type(req, TestFuseReplyType::ENTRY);

                AND_THEN("The entry has a distinct inode and is of regular file format") {
                    auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                    CHECK(entry.ino != Dragonstash::ROOT_INO);
                    CHECK(entry.ino != Dragonstash::INVALID_INO);
                    CHECK((entry.attr.st_mode & S_IFMT) == S_IFREG);
                }

                AND_THEN("The entry has correct attributes") {
                    auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                    CHECK(entry.attr.st_uid == env.default_uid());
                    CHECK(entry.attr.st_gid == env.default_gid());
                    CHECK((entry.attr.st_mode & S_IRWXU) == (S_IRUSR | S_IWUSR));
                    CHECK((entry.attr.st_mode & S_IRWXG) == (S_IRGRP));
                    CHECK((entry.attr.st_mode & S_IRWXO) == 0);
                    CHECK(entry.attr.st_mtim.tv_sec == env.default_timestamp().tv_sec);
                    CHECK(entry.attr.st_mtim.tv_nsec == env.default_timestamp().tv_nsec);
                }
            }

            AND_WHEN("Re-requesting the same file") {
                check_reply_type(req, TestFuseReplyType::ENTRY);
                auto entry_1 = std::get<TestFuseReplyEntry>(req.reply_argv());

                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");

                THEN("Its inode number is unchanged") {
                    check_reply_type(req, TestFuseReplyType::ENTRY);
                    auto entry_2 = std::get<TestFuseReplyEntry>(req.reply_argv());

                    CHECK(entry_1.ino == entry_2.ino);
                }
            }
        }

        WHEN("Requesting to look up a nonexistent file") {
            auto req = env.fuse().new_request();
            fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "random name");

            THEN("The FS replies with ENOENT") {
                check_reply_error(req, ENOENT);
            }
        }

        WHEN("Requesting to look up a directory") {
            auto req = env.fuse().new_request();
            fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "books");

            THEN("The FS replies with an entry") {
                check_reply_type(req, TestFuseReplyType::ENTRY);

                AND_THEN("The entry has a distinct inode and is of directory format") {
                    auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                    CHECK(entry.ino != Dragonstash::ROOT_INO);
                    CHECK(entry.ino != Dragonstash::INVALID_INO);
                    CHECK((entry.attr.st_mode & S_IFMT) == S_IFDIR);
                }

                AND_THEN("The entry has correct attributes") {
                    auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                    CHECK(entry.attr.st_uid == env.default_uid());
                    CHECK(entry.attr.st_gid == env.default_gid());
                    CHECK((entry.attr.st_mode & S_IRWXU) == (S_IRUSR | S_IWUSR | S_IXUSR));
                    CHECK((entry.attr.st_mode & S_IRWXG) == (S_IRGRP | S_IXGRP));
                    CHECK((entry.attr.st_mode & S_IRWXO) == 0);
                    CHECK(entry.attr.st_mtim.tv_sec == env.default_timestamp().tv_sec);
                    CHECK(entry.attr.st_mtim.tv_nsec == env.default_timestamp().tv_nsec);
                }
            }

            THEN("The inode is different from the inode of the file") {
                auto req_file = env.fuse().new_request();
                fs.lookup(req_file.wrap(), Dragonstash::ROOT_INO, "README.md");
                check_reply_type(req, TestFuseReplyType::ENTRY);
                check_reply_type(req_file, TestFuseReplyType::ENTRY);

                CHECK(std::get<TestFuseReplyEntry>(req.reply_argv()).ino !=
                        std::get<TestFuseReplyEntry>(req_file.reply_argv()).ino);
            }
        }

        WHEN("Setting the backend to be disconnected") {
            {
                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");
                check_reply_type(req, TestFuseReplyType::ENTRY);
            }

            env.backend().set_connected(false);

            AND_WHEN("Looking up an uncached inode") {
                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "books");

                THEN("EIO is returned") {
                    check_reply_error(req, EIO);
                }
            }

            AND_WHEN("Looking up a cached inode") {
                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");

                THEN("The entry is returned") {
                    check_reply_type(req, TestFuseReplyType::ENTRY);

                    auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                    CHECK(entry.attr.st_mode == (S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP));
                    CHECK(entry.attr.st_uid == env.default_uid());
                    CHECK(entry.attr.st_gid == env.default_gid());
                }
            }
        }

        WHEN("Looking up an existing file") {
            auto req = env.fuse().new_request();
            fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");
            check_reply_type(req, TestFuseReplyType::ENTRY);

            AND_WHEN("Looking up the file again after removing it from the backend") {
                env.backend().remove("README.md");

                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");

                THEN("The FS replies with ENOENT") {
                    check_reply_error(req, ENOENT);
                }

                THEN("The file is removed from the cache") {
                    check_result_error(env.cache().lookup(Dragonstash::ROOT_INO, "README.md"), ENOENT);
                }

                AND_WHEN("Setting the backend to disconnected and trying the lookup again") {
                    env.backend().set_connected(false);

                    auto req = env.fuse().new_request();
                    fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");

                    THEN("The lookup fails with EIO (because not fully synced)") {
                        check_reply_error(req, EIO);
                    }
                }
            }
        }
    }
}

SCENARIO("opendir and readdir") {
    TestEnvironment env;

    GIVEN("A filesystem with default contents") {
        env.with_default_contents();
        Dragonstash::Filesystem &fs = env.fs();

        WHEN("Testing the synced flag of the root directory") {
            THEN("It is unset") {
                auto flag_result = env.cache().begin_ro().test_flag(Dragonstash::ROOT_INO, Dragonstash::InodeFlag::SYNCED);
                require_result_ok(flag_result);
                CHECK(!*flag_result);
            }
        }

        WHEN("Opening the root directory with opendir") {
            auto req = env.fuse().new_request();
            struct fuse_file_info fi{};
            fs.opendir(req.wrap(), Dragonstash::ROOT_INO, &fi);

            THEN("The call succeeds and returns using fuse_reply_open") {
                check_reply_type(req, TestFuseReplyType::OPEN);
            }

            THEN("The root directory is marked as synced") {
                auto flag_result = env.cache().begin_ro().test_flag(Dragonstash::ROOT_INO, Dragonstash::InodeFlag::SYNCED);
                require_result_ok(flag_result);
                CHECK(*flag_result);
            }

            THEN("Child directories are not marked as synced") {
                auto txn = env.cache().begin_ro();
                auto lookup_result = txn.lookup(Dragonstash::ROOT_INO, "books");
                require_result_ok(lookup_result);

                auto flag_result = txn.test_flag(*lookup_result, Dragonstash::InodeFlag::SYNCED);
                require_result_ok(flag_result);
                CHECK(!*flag_result);
            }

            THEN("Dot cannot be looked up in the cache") {
                check_result_error(env.cache().lookup(Dragonstash::ROOT_INO, "."), ENOENT);
            }

            THEN("Dotdot cannot be looked up in the cache") {
                check_result_error(env.cache().lookup(Dragonstash::ROOT_INO, ".."), ENOENT);
            }

            AND_WHEN("Setting the backend to disconnected") {
                env.backend().set_connected(false);

                AND_WHEN("Calling lookup on an existing entry") {
                    auto req = env.fuse().new_request();
                    fs.lookup(req, Dragonstash::ROOT_INO, "README.md");

                    THEN("The call succeds and it returns attributes") {
                        check_reply_type(req, TestFuseReplyType::ENTRY);
                        auto entry = std::get<TestFuseReplyEntry>(req.reply_argv());
                        CHECK(entry.attr.st_mode == (S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP));
                    }
                }

                AND_WHEN("Opening the root directory with opendir again") {
                    auto req = env.fuse().new_request();
                    struct fuse_file_info fi{};
                    fs.opendir(req.wrap(), Dragonstash::ROOT_INO, &fi);

                    THEN("The call succeeds and returns using fuse_reply_open") {
                        check_reply_type(req, TestFuseReplyType::OPEN);
                    }

                    AND_WHEN("Iterating it") {
                        check_reply_type(req, TestFuseReplyType::OPEN);
                        fuse_file_info fi = std::get<TestFuseReplyOpen>(req.reply_argv());

                        THEN("It returns data after dotdot") {
                            // XXX: this heavily relies on implementation
                            // details of the cache because we cannot
                            // deserialise the dir entry format used by fus

                            // To get the entry after dotdot, we have to ask
                            // starting at the offset equal to the parent inode
                            req = env.fuse().new_request();
                            const std::size_t size = 4096;
                            fs.readdir(req.wrap(), Dragonstash::ROOT_INO, size, Dragonstash::ROOT_INO, &fi);
                            check_reply_type(req, TestFuseReplyType::BUF);
                        }
                    }
                }

                AND_WHEN("Opening an uncached directory with opendir") {
                    auto req = env.fuse().new_request();
                    fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "books");
                    check_reply_type(req, TestFuseReplyType::ENTRY);

                    ino_t dir_ino = std::get<TestFuseReplyEntry>(req.reply_argv()).ino;

                    req = env.fuse().new_request();
                    struct fuse_file_info fi{};
                    fs.opendir(req.wrap(), dir_ino, &fi);

                    THEN("The call succeeds") {
                        check_reply_type(req, TestFuseReplyType::OPEN);
                    }

                    AND_WHEN("Iterating it") {
                        check_reply_type(req, TestFuseReplyType::OPEN);
                        fuse_file_info fi = std::get<TestFuseReplyOpen>(req.reply_argv());

                        THEN("It returns EIO after dotdot") {
                            // XXX: this heavily relies on implementation
                            // details of the cache because we cannot
                            // deserialise the dir entry format used by fus

                            // To get the entry after dotdot, we have to ask
                            // starting at the offset equal to the parent inode
                            req = env.fuse().new_request();
                            const std::size_t size = 4096;
                            fs.readdir(req.wrap(), dir_ino, size, Dragonstash::ROOT_INO, &fi);
                            check_reply_error(req, EIO);
                        }
                    }
                }
            }

            AND_WHEN("Calling opendir again") {
                auto lookup_result_1 = env.cache().lookup(Dragonstash::ROOT_INO, "README.md");
                require_result_ok(lookup_result_1);
                auto lookup_result_2 = env.cache().lookup(Dragonstash::ROOT_INO, "books");
                require_result_ok(lookup_result_2);

                auto req = env.fuse().new_request();
                struct fuse_file_info fi{};
                fs.opendir(req.wrap(), Dragonstash::ROOT_INO, &fi);
                check_reply_type(req, TestFuseReplyType::OPEN);

                THEN("Inode numbers do not change") {
                    auto lookup_result_1_test = env.cache().lookup(Dragonstash::ROOT_INO, "README.md");
                    require_result_ok(lookup_result_1_test);
                    auto lookup_result_2_test = env.cache().lookup(Dragonstash::ROOT_INO, "books");
                    require_result_ok(lookup_result_2_test);

                    CHECK(*lookup_result_1 == *lookup_result_1_test);
                    CHECK(*lookup_result_2 == *lookup_result_2_test);
                }
            }

            AND_WHEN("Removing a file from the backend and calling opendir again") {
                env.backend().remove("README.md");

                auto lookup_result_dir = env.cache().lookup(Dragonstash::ROOT_INO, "books");
                require_result_ok(lookup_result_dir);

                auto req = env.fuse().new_request();
                struct fuse_file_info fi{};
                fs.opendir(req.wrap(), Dragonstash::ROOT_INO, &fi);

                AND_WHEN("Disconnecting the backend") {
                    env.backend().set_connected(false);

                    THEN("Lookup for the removed file returns ENOENT") {
                        auto req = env.fuse().new_request();
                        fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");
                        check_reply_error(req, ENOENT);
                    }

                    THEN("The inode number for the directory is still intact") {
                        auto lookup_result_dir_test = env.cache().lookup(Dragonstash::ROOT_INO, "books");
                        require_result_ok(lookup_result_dir_test);

                        CHECK(*lookup_result_dir == *lookup_result_dir_test);
                    }
                }
            }

            AND_WHEN("Removing a file from the backend and calling lookup") {
                env.backend().remove("README.md");

                auto req = env.fuse().new_request();
                fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");
                check_reply_error(req, ENOENT);

                THEN("The file is removed from the cache") {
                    check_result_error(env.cache().lookup(Dragonstash::ROOT_INO, "README.md"), ENOENT);
                }

                AND_WHEN("Disconnecting the backend") {
                    env.backend().set_connected(false);

                    THEN("Lookup for the removed file returns ENOENT") {
                        auto req = env.fuse().new_request();
                        fs.lookup(req.wrap(), Dragonstash::ROOT_INO, "README.md");
                        check_reply_error(req, ENOENT);
                    }
                }
            }
        }
    }
}

SCENARIO("readlink") {
    TestEnvironment env;

    GIVEN("A filesystem with default contents") {
        env.with_default_contents();
        Dragonstash::Filesystem &fs = env.fs();

        auto dir_lookup_result = lookup(env.fuse(), env.fs(), Dragonstash::ROOT_INO, "books");
        require_result_ok(dir_lookup_result);
        const ino_t dir_ino = *dir_lookup_result;

        auto link_lookup_result = lookup(env.fuse(), env.fs(), dir_ino, "best.epub");
        require_result_ok(link_lookup_result);
        const ino_t link_ino = *link_lookup_result;

        WHEN("Calling readlink on a directory") {
            auto req = env.fuse().new_request();
            env.fs().readlink(req.wrap(), dir_ino);

            THEN("EINVAL is returned") {
                check_reply_error(req, EINVAL);
            }
        }

        WHEN("Calling readlink on a file") {
            auto lookup_result = lookup(env.fuse(), env.fs(), Dragonstash::ROOT_INO, "README.md");
            require_result_ok(lookup_result);

            auto req = env.fuse().new_request();
            env.fs().readlink(req.wrap(), *lookup_result);

            THEN("EINVAL is returned") {
                check_reply_error(req, EINVAL);
            }
        }

        WHEN("Calling readlink on a link") {
            auto req = env.fuse().new_request();
            env.fs().readlink(req.wrap(), link_ino);

            THEN("The destination is returned") {
                check_reply_type(req, TestFuseReplyType::READLINK);
                auto dest = std::get<TestFuseReplyReadlink>(req.reply_argv());
                CHECK(dest == "Hitchhiker's Guide To The Galaxy.epub");
            }

            AND_WHEN("Setting the backend to disconnected and calling readlink again") {
                env.backend().set_connected(false);

                auto req = env.fuse().new_request();
                env.fs().readlink(req.wrap(), link_ino);

                THEN("The destination is returned") {
                    check_reply_type(req, TestFuseReplyType::READLINK);
                    auto dest = std::get<TestFuseReplyReadlink>(req.reply_argv());
                    CHECK(dest == "Hitchhiker's Guide To The Galaxy.epub");
                }
            }

            AND_WHEN("Replacing the link with a directory, calling readlink and then setting the backend to disconnected") {
                auto find_result = env.backend().find("/books");
                require_result_ok(find_result);

                auto dir = dynamic_cast<Dragonstash::Backend::InMemory::Directory*>(*find_result);
                dir->remove("best.epub");
                dir->emplace<Dragonstash::Backend::InMemory::File>("best.epub");

                auto req = env.fuse().new_request();
                env.fs().readlink(req.wrap(), link_ino);
                check_reply_error(req, EINVAL);

                env.backend().set_connected(false);

                THEN("readlink returns an error (although it is unspecified which)") {
                    auto req = env.fuse().new_request();
                    env.fs().readlink(req.wrap(), link_ino);
                    check_reply_type(req, TestFuseReplyType::ERROR);
                }
            }
        }
    }
}
