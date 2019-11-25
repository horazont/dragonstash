/**********************************************************************
File name: in_memory.cpp
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
#include <sys/types.h>
#include <fcntl.h>
#include <iostream>

#include "dragonstash/backend/in_memory.hpp"

using namespace Dragonstash::Backend;

SCENARIO("Node construction and basic operations") {
    InMemoryFilesystem fs;
    GIVEN("An empty fs") {
        WHEN("Calling lstat with an empty path") {
            auto lstat_result = fs.lstat("");

            THEN("It should fail with EINVAL") {
                CHECK(lstat_result.error() == EINVAL);
                CHECK(!lstat_result);
            }
        }

        WHEN("Calling lstat on the root") {
            auto lstat_result = fs.lstat("/");

            THEN("The call should succeed") {
                CHECK(lstat_result.error() == 0);
                REQUIRE(lstat_result);

                AND_THEN("The format should be a directory") {
                    CHECK((lstat_result->mode & S_IFMT) == S_IFDIR);
                }
            }
        }
    }

    GIVEN("A directory at /foo") {
        fs.emplace<InMemory::Directory>("foo");

        WHEN("Calling lstat") {
            auto lstat_result = fs.lstat("/foo");

            THEN("The call should succeed") {
                CHECK(lstat_result.error() == 0);
                REQUIRE(lstat_result);

                AND_THEN("The format should be a directory") {
                    CHECK((lstat_result->mode & S_IFMT) == S_IFDIR);
                }
            }
        }

        WHEN("Calling readlink") {
            auto readlink_result = fs.readlink("/foo");

            THEN("The call should fail with EINVAL") {
                CHECK(readlink_result.error() == EINVAL);
                REQUIRE(!readlink_result);
            }
        }

        WHEN("Calling open") {
            auto open_result = fs.open("/foo", O_RDWR, 0);

            THEN("The call should fail with EISDIR") {
                CHECK(open_result.error() == EISDIR);
                REQUIRE(!open_result);
            }
        }

        AND_WHEN("Adding a nested file") {
            auto dir_raw = fs.find("/foo");
            REQUIRE(dir_raw);
            auto &dir = *dynamic_cast<InMemory::Directory*>(*dir_raw);

            dir.emplace<InMemory::File>("f1");

            THEN("Calling lstat on the nested file succeods") {
                auto result = fs.lstat("/foo/f1");
                CHECK(result.error() == 0);
                CHECK(result);
            }
        }
    }

    GIVEN("A file at /foo") {
        fs.emplace<InMemory::File>("foo");

        WHEN("Calling lstat") {
            auto lstat_result = fs.lstat("/foo");

            THEN("The call should succeed") {
                CHECK(lstat_result.error() == 0);
                REQUIRE(lstat_result);

                AND_THEN("The format should be a regular file") {
                    CHECK((lstat_result->mode & S_IFMT) == S_IFREG);
                }
            }
        }

        WHEN("Calling readlink") {
            auto readlink_result = fs.readlink("/foo");

            THEN("The call should fail with EINVAL") {
                CHECK(readlink_result.error() == EINVAL);
                REQUIRE(!readlink_result);
            }
        }

        WHEN("Calling opendir") {
            auto opendir_result = fs.opendir("/foo");

            THEN("The call should fail with ENOTDIR") {
                CHECK(opendir_result.error() == ENOTDIR);
                REQUIRE(!opendir_result);
            }
        }
    }

    GIVEN("A link at /foo") {
        auto &link = fs.emplace<InMemory::Link>("foo", "some destination");

        WHEN("Calling lstat") {
            auto lstat_result = fs.lstat("/foo");

            THEN("The call should succeed") {
                CHECK(lstat_result.error() == 0);
                REQUIRE(lstat_result);

                AND_THEN("The format should be a link") {
                    CHECK((lstat_result->mode & S_IFMT) == S_IFLNK);
                }
            }
        }

        WHEN("Calling readlink") {
            auto readlink_result = fs.readlink("/foo");

            THEN("The call should succeed") {
                CHECK(readlink_result.error() == 0);
                REQUIRE(readlink_result);

                AND_THEN("The destination should be correct") {
                    CHECK(*readlink_result == "some destination");
                }
            }
        }

        WHEN("Calling open") {
            auto open_result = fs.open("/foo", O_RDWR, 0);

            THEN("The call should fail with EINVAL") {
                CHECK(open_result.error() == EINVAL);
                REQUIRE(!open_result);
            }
        }

        WHEN("Calling opendir") {
            auto opendir_result = fs.opendir("/foo");

            THEN("The call should fail with ENOTDIR") {
                CHECK(opendir_result.error() == ENOTDIR);
                REQUIRE(!opendir_result);
            }
        }
    }
}

SCENARIO("Directory iteration") {
    InMemoryFilesystem fs;

    GIVEN("An empty fs") {
        WHEN("Calling opendir on the root") {
            auto opendir_result = fs.opendir("/");

            THEN("The call succeeds") {
                CHECK(opendir_result.error() == 0);
                CHECK(opendir_result);
            }
        }

        WHEN("Iterating the root directory") {
            auto opendir_result = fs.opendir("/");
            REQUIRE(opendir_result);
            auto &handle = **opendir_result;

            THEN("The entries are . and ..") {
                auto readdir_result = handle.readdir();
                CHECK(readdir_result.error() == 0);
                REQUIRE(readdir_result);
                CHECK(readdir_result->name == ".");
                CHECK(readdir_result->ino == 0);
                CHECK(!readdir_result->complete);

                readdir_result = handle.readdir();
                CHECK(readdir_result.error() == 0);
                REQUIRE(readdir_result);
                CHECK(readdir_result->name == "..");
                CHECK(readdir_result->ino == 0);
                CHECK(!readdir_result->complete);

                readdir_result = handle.readdir();
                CHECK(readdir_result.error() == 0);
                CHECK(!readdir_result);
            }
        }
    }

    GIVEN("An fs with a couple of entries in the root") {
        auto &d1 = fs.emplace<InMemory::Directory>("d1");
        auto &f1 = fs.emplace<InMemory::File>("f1");

        WHEN("Iterating the root directory") {
            auto opendir_result = fs.opendir("/");
            REQUIRE(opendir_result);
            auto &handle = **opendir_result;

            THEN("The first two entries are . and ..") {
                auto readdir_result = handle.readdir();
                CHECK(readdir_result.error() == 0);
                REQUIRE(readdir_result);
                CHECK(readdir_result->name == ".");

                readdir_result = handle.readdir();
                CHECK(readdir_result.error() == 0);
                REQUIRE(readdir_result);
                CHECK(readdir_result->name == "..");

                AND_THEN("The entries are /d1 and /f1, in undefined order") {
                    std::vector<DirEntry> entries;
                    while (readdir_result = handle.readdir()) {
                        entries.emplace_back(*readdir_result);
                    }
                    CHECK(readdir_result.error() == 0);

                    CHECK(entries.size() == 2);
                    std::sort(entries.begin(), entries.end(),
                              [](const DirEntry &a, const DirEntry &b){
                        return a.name < b.name;
                    });

                    CHECK(entries[0].name == "d1");
                    CHECK(entries[1].name == "f1");
                }
            }
        }
    }
}

SCENARIO("File I/O") {
    InMemoryFilesystem fs;

    GIVEN("A fs with a file") {
        fs.emplace<InMemory::File>("f1");

        WHEN("Calling lstat") {
            auto lstat_result = fs.lstat("/f1");

            THEN("The initial size is zero") {
                CHECK(lstat_result.error() == 0);
                REQUIRE(lstat_result);
                CHECK(lstat_result->size == 0);
            }
        }

        WHEN("Opening the file for reading") {
            auto open_result = fs.open("/f1", O_RDONLY, 0);

            THEN("The open succeds") {
                CHECK(open_result.error() == 0);
                CHECK(open_result);
            }

            AND_WHEN("Calling fstat") {
                auto fstat_result = (*open_result)->fstat();

                THEN("fstat returns an initial size of zero") {
                    CHECK(fstat_result.error() == 0);
                    REQUIRE(fstat_result);
                    CHECK(fstat_result->size == 0);
                }
            }
        }

        WHEN("Opening the file for read/write") {
            auto open_result = fs.open("/f1", O_RDWR, 0);

            THEN("The open succeds") {
                CHECK(open_result.error() == 0);
                CHECK(open_result);
            }

            AND_WHEN("Writing to a location") {
                const std::string data("random data");
                const off_t offset = 2371;

                REQUIRE(open_result);
                auto &handle = **open_result;

                auto write_result = handle.pwrite(data.data(), data.size(),
                                                  offset);

                THEN("The write succeeds and indicates the number of bytes written") {
                    CHECK(write_result.error() == 0);
                    REQUIRE(write_result);
                    CHECK(*write_result == data.size());
                }

                THEN("The size grows to cover the full write") {
                    auto fstat_result = (*open_result)->fstat();
                    CHECK(fstat_result.error() == 0);
                    REQUIRE(fstat_result);
                    CHECK(fstat_result->size == offset + data.size());
                }

                AND_WHEN("Reading the contents back") {
                    std::string outbuf;
                    outbuf.resize(data.size());

                    auto read_result = handle.pread(outbuf.data(), outbuf.size(),
                                                    offset);

                    THEN("The read succeeds") {
                        CHECK(read_result.error() == 0);
                        CHECK(read_result);
                    }

                    THEN("It indicates the correct size") {
                        REQUIRE(read_result);
                        CHECK(*read_result == outbuf.size());
                    }

                    THEN("The read data is equal to the written data") {
                        CHECK(outbuf == data);
                    }
                }

                AND_WHEN("Reading before the contents") {
                    std::string outbuf("xxxxxxxxxxxxxxxxx");

                    auto read_result = handle.pread(outbuf.data(), outbuf.size(),
                                                    offset - 100);

                    THEN("The read succeeds") {
                        CHECK(read_result.error() == 0);
                        CHECK(read_result);
                    }

                    THEN("It indicates the correct size") {
                        REQUIRE(read_result);
                        CHECK(*read_result == outbuf.size());
                    }

                    THEN("The read data is all zeroes") {
                        for (auto ch: outbuf) {
                            CHECK(ch == 0);
                        }
                    }
                }
            }
        }
    }

    GIVEN("An fs with an open file") {
        fs.emplace<InMemory::File>("f1");
        auto open_result = fs.open("/f1", O_RDWR, 0);
        REQUIRE(open_result);
        auto &handle = **open_result;

        WHEN("Calling fsync") {
            auto fsync_result = handle.fsync();

            THEN("The call succeeds") {
                CHECK(fsync_result.error() == 0);
                CHECK(fsync_result);
            }
        }

        WHEN("Calling close") {
            auto close_result = handle.close();

            THEN("The call succeeds") {
                CHECK(close_result.error() == 0);
                CHECK(close_result);
            }
        }
    }

    GIVEN("An fs with an open file with data") {
        fs.emplace<InMemory::File>("f1");
        auto open_result = fs.open("/f1", O_RDWR, 0);
        REQUIRE(open_result);
        auto &handle = **open_result;

        const std::string data("some random data");
        {
            auto write_result = handle.pwrite(data.data(), data.size(), 0);
            CHECK(write_result.error() == 0);
            REQUIRE(write_result);
            CHECK(*write_result == data.size());
        }

        WHEN("Reading starting beyond the end of file") {
            std::string buf("xxxxxxxxxxxxxxxxx");
            const std::string copy(buf);
            auto read_result = handle.pread(buf.data(), buf.size(), 1024);

            THEN("The read succeeds") {
                CHECK(read_result.error() == 0);
                CHECK(read_result);
            }

            THEN("It reads zero bytes") {
                REQUIRE(read_result);
                CHECK(*read_result == 0);
            }

            THEN("The buffer is unchanged") {
                CHECK(buf == copy);
            }
        }

        WHEN("Reading at the end of file") {
            std::string buf("xxxxxxxxxxxxxxxxx");
            const std::string copy(buf);
            auto read_result = handle.pread(buf.data(), buf.size(), data.size());

            THEN("The read succeeds") {
                CHECK(read_result.error() == 0);
                CHECK(read_result);
            }

            THEN("It reads zero bytes") {
                REQUIRE(read_result);
                CHECK(*read_result == 0);
            }

            THEN("The buffer is unchanged") {
                CHECK(buf == copy);
            }
        }

        WHEN("Reading beyond the end of file") {
            std::string buf("xxxxxxxxxxxxxxxxx");
            const std::string copy(buf);
            auto read_result = handle.pread(buf.data(), buf.size(), data.size()-4);

            THEN("The read succeeds") {
                CHECK(read_result.error() == 0);
                CHECK(read_result);
            }

            THEN("The read is truncated") {
                REQUIRE(read_result);
                CHECK(*read_result == 4);
            }

            THEN("The buffer is only changed in the read bytes") {
                std::string read_data;
                read_data.resize(buf.size());

                std::copy(data.end() - 4, data.end(),
                          &read_data[0]);
                std::copy(copy.begin(), copy.end() - 4,
                          &read_data[4]);

                CHECK(read_data == buf);
            }
        }
    }
}

SCENARIO("Entry removal") {
    GIVEN("A fs with a directory and two files") {
        InMemoryFilesystem fs;
        fs.emplace<InMemory::File>("f1");
        fs.emplace<InMemory::Directory>("dir").emplace<InMemory::File>("f2");

        WHEN("Removing the directory") {
            fs.remove("dir");

            THEN("lstat on it fails with ENOENT") {
                auto result = fs.lstat("/dir");
                CHECK(result.error() == ENOENT);
                CHECK(!result);
            }

            THEN("lstat on its contents fails with ENOENT") {
                auto result = fs.lstat("/dir/f2");
                CHECK(result.error() == ENOENT);
                CHECK(!result);
            }

            THEN("Attempting to opendir it fails with ENOENT") {
                auto result = fs.opendir("/dir");
                CHECK(result.error() == ENOENT);
                CHECK(!result);
            }

            AND_WHEN("lstat on the intact file") {
                auto result = fs.lstat("/f1");

                THEN("It succeds") {
                    CHECK(result.error() == 0);
                    CHECK(result);
                }
            }
        }

        WHEN("Removing the file") {
            fs.remove("f1");

            THEN("lstat on it fails with ENOENT") {
                auto result = fs.lstat("/f1");
                CHECK(result.error() == ENOENT);
                CHECK(!result);
            }

            THEN("Attempting to open it fails with ENOENT") {
                auto result = fs.open("/f1", O_RDWR, 0);
                CHECK(result.error() == ENOENT);
                CHECK(!result);
            }

            AND_WHEN("lstat on the intact directory") {
                auto result = fs.lstat("/dir");

                THEN("It succeds") {
                    CHECK(result.error() == 0);
                    CHECK(result);
                }
            }

            AND_WHEN("lstat on the intact file") {
                auto result = fs.lstat("/dir/f2");

                THEN("It succeds") {
                    CHECK(result.error() == 0);
                    CHECK(result);
                }
            }
        }
    }
}
