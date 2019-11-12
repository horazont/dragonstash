/**********************************************************************
File name: blocklist.cpp
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

#include <iostream>

#include "cache/common.hpp"
#include "cache/blocklist.hpp"

#include "testutils/tempdir.hpp"


class TestBlocklist
{
public:
    TestBlocklist():
        m_blocklist(m_tempdir.path() / "blocklist")
    {
        CHECK(m_blocklist.nentries() == 0);
    }

    ~TestBlocklist()
    {
        try {
            m_blocklist.fsck();
        } catch (std::runtime_error &err) {
            m_blocklist.dump(std::cerr);
            std::unexpected();
        }
    }

private:
    TemporaryDirectory m_tempdir;
    Dragonstash::Blocklist m_blocklist;

public:
    inline Dragonstash::Blocklist &blocklist()
    {
        return m_blocklist;
    }

};


TEST_CASE("Defaults of an empty cache", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    CHECK(blist.present_blocks() == 0);
    CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 0);
    CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 0);
    CHECK(blist.blocks(Dragonstash::Blocklist::WRITTEN) == 0);
    CHECK(blist.blocks(Dragonstash::Blocklist::PINNED) == 0);
    CHECK(blist.size() == 0);
    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.capacity() > 0);
    CHECK(blist.nentries() == 0);
}

TEST_CASE("Mark a page as readahead", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Mark a page range as readahead", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 3, Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Noop mark", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 0, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 0);
}

TEST_CASE("Change marking of a page", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(2, 1, Dragonstash::Blocklist::READ);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Change marking of a single page out of a previously marked range",
          "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 3, Dragonstash::Blocklist::READAHEAD);
    blist.mark(3, 1, Dragonstash::Blocklist::READ);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 3);
}

TEST_CASE("Join ranges of marked pages if compatible state", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(4, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(3, 1, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Do not join ranges of marked pages if incompatible state", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(4, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(3, 1, Dragonstash::Blocklist::READ);

    CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 3);
}

TEST_CASE("Join ranges of marked pages on partial state change", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 2, Dragonstash::Blocklist::READ);
    blist.mark(1, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(4, 1, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    blist.mark(3, 1, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 3);
}

TEST_CASE("Join ranges of marked pages on exact hit state change", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READ);
    blist.mark(1, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(3, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Partially change state of existing range", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 3, Dragonstash::Blocklist::READAHEAD);
    blist.mark(1, 2, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 2);
}

TEST_CASE("Completely override following range", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 3, Dragonstash::Blocklist::READAHEAD);
    blist.mark(1, 5, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(5) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Override multiple independent ranges", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(4, 1, Dragonstash::Blocklist::READAHEAD);
    blist.mark(1, 5, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(5) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 1);
}

TEST_CASE("Force grow of mapping", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    auto initial_capacity = blist.capacity();

    for (decltype(initial_capacity) i = 0; i < initial_capacity + 1; ++i) {
        blist.mark(i * 2, 1, Dragonstash::Blocklist::READ);
        REQUIRE(blist.nentries() == i + 1);
    }

    CHECK(blist.capacity() > initial_capacity);
    CHECK(blist.nentries() == initial_capacity+1);

    for (decltype(initial_capacity) i = 0; i < initial_capacity + 1; ++i) {
        CHECK(blist.state(i*2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(i*2+1) == Dragonstash::Blocklist::ABSENT);
    }
}

TEST_CASE("Mark base cases", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(1, 3, Dragonstash::Blocklist::READ);
    blist.mark(7, 3, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 2);

    CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 0);
    CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 6);
    CHECK(blist.present_blocks() == 6);

    SECTION("Case 1: neither start nor end overlap") {
        blist.mark(4, 3, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(5) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 3);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 3);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 6);
        CHECK(blist.present_blocks() == 9);
    }

    SECTION("Case 2: start overlaps, end does not") {
        blist.mark(3, 4, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(5) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 3);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 4);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 9);
    }

    SECTION("Case 3: both overlap, but are not equal") {
        blist.mark(3, 5, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(5) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 3);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 5);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 4);
        CHECK(blist.present_blocks() == 9);
    }

    SECTION("Case 4: end overlaps, start does not") {
        blist.mark(4, 4, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(5) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 3);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 4);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 9);
    }

    SECTION("Case 5: fully covered by existing range") {
        blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 4);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 1);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 6);
    }
}

TEST_CASE("Unmark base cases", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(1, 3, Dragonstash::Blocklist::READ);
    blist.mark(7, 3, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 2);

    CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 6);
    CHECK(blist.present_blocks() == 6);

    SECTION("Case 1: neither start nor end overlap") {
        blist.mark(4, 3, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 6);
        CHECK(blist.present_blocks() == 6);
    }

    SECTION("Case 2: start overlaps, end does not") {
        blist.mark(3, 4, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 5);
    }

    SECTION("Case 3: both overlap, but are not equal") {
        blist.mark(3, 5, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 4);
        CHECK(blist.present_blocks() == 4);
    }

    SECTION("Case 4: end overlaps, start does not") {
        blist.mark(4, 4, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 5);
    }

    SECTION("Case 5: fully covered by existing range") {
        blist.mark(2, 1, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 3);

        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 5);
        CHECK(blist.present_blocks() == 5);
    }
}

TEST_CASE("Mark edge cases", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(1, 3, Dragonstash::Blocklist::READ);
    blist.mark(7, 3, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 2);

    SECTION("Exactly matching range") {
        blist.mark(1, 3, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);
    }

    SECTION("Overriding range") {
        blist.mark(6, 5, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(10) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(11) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);
    }

    SECTION("Covering multiple existing ranges") {
        blist.mark(0, 11, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(4) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(5) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(6) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(10) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(11) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 1);
    }
}

TEST_CASE("Unmark edge cases", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(1, 3, Dragonstash::Blocklist::READ);
    blist.mark(7, 3, Dragonstash::Blocklist::READ);

    CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
    CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 2);

    SECTION("Exactly matching range") {
        blist.mark(1, 3, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(2) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 1);
    }

    SECTION("Overriding range") {
        blist.mark(6, 5, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(8) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(9) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(11) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 1);
    }

    SECTION("Covering multiple existing ranges") {
        blist.mark(0, 11, Dragonstash::Blocklist::ABSENT);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(2) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(3) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(8) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(9) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(11) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 0);
    }
}

TEST_CASE("Reload Blocklist from file")
{
    TemporaryDirectory tdir;
    const std::filesystem::path fname = tdir.path() / "blocklist";

    {
        Dragonstash::Blocklist blist(fname);

        blist.mark(1, 3, Dragonstash::Blocklist::READ);
        blist.mark(7, 3, Dragonstash::Blocklist::READAHEAD);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 3);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 3);
        CHECK(blist.present_blocks() == 6);
    }

    {
        Dragonstash::Blocklist blist(fname);

        CHECK(blist.state(0) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(1) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(2) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(3) == Dragonstash::Blocklist::READ);
        CHECK(blist.state(4) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(5) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(6) == Dragonstash::Blocklist::ABSENT);
        CHECK(blist.state(7) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(8) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(9) == Dragonstash::Blocklist::READAHEAD);
        CHECK(blist.state(10) == Dragonstash::Blocklist::ABSENT);

        CHECK(blist.nentries() == 2);

        CHECK(blist.blocks(Dragonstash::Blocklist::READAHEAD) == 3);
        CHECK(blist.blocks(Dragonstash::Blocklist::READ) == 3);
        CHECK(blist.present_blocks() == 6);
    }
}

SCENARIO("Truncation of access attempts", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    GIVEN("A fully absent blocklist") {
        WHEN("Truncating the access for an absent block") {
            THEN("truncate_access returns zero length") {
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE-1, 456) == 0);
            }
        }
    }

    GIVEN("A blocklist with a single present block") {
        blist.mark(1, 1, Dragonstash::Blocklist::READ);

        WHEN("Truncating access for an absent block") {
            THEN("truncate_access returns zero length") {
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE-1, 456) == 0);
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE*2, 456) == 0);
            }
        }

        WHEN("Truncating access starting at the block start and equal to block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE) ==
                      Dragonstash::CACHE_PAGE_SIZE);
            }
        }

        WHEN("Truncating access starting at the block start and with a length less than block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE - 1) ==
                      Dragonstash::CACHE_PAGE_SIZE - 1);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE / 2) ==
                      Dragonstash::CACHE_PAGE_SIZE / 2);
            }
        }

        WHEN("Truncating access starting at the block start and with a length larger than block size") {
            THEN("The size is truncated to block size") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE + 1) ==
                      Dragonstash::CACHE_PAGE_SIZE);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE * 2) ==
                      Dragonstash::CACHE_PAGE_SIZE);
            }
        }
    }

    GIVEN("A blocklist with a sequence of equal-state present blocks") {
        blist.mark(1, 3, Dragonstash::Blocklist::READ);

        WHEN("Truncating access for an absent block") {
            THEN("truncate_access returns zero length") {
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE-1, 456) == 0);
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE*5, 456) == 0);
            }
        }

        WHEN("Truncating access starting at the block start and equal to block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE) ==
                      Dragonstash::CACHE_PAGE_SIZE);
            }
        }

        WHEN("Truncating access starting at the block start and with a length less than block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE - 1) ==
                      Dragonstash::CACHE_PAGE_SIZE - 1);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE / 2) ==
                      Dragonstash::CACHE_PAGE_SIZE / 2);
            }
        }

        WHEN("Truncating access starting at the block start and with a length larger than block size but smaller than range length") {
            THEN("The original size is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE + 1) ==
                      Dragonstash::CACHE_PAGE_SIZE+1);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE * 3) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
            }
        }

        WHEN("Truncating access starting at the block start and with a length larger than range length") {
            THEN("The size is truncated to the end of range") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE*3+1) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE * 4) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
            }
        }
    }

    GIVEN("A blocklist with a sequence of non-equal-state present blocks") {
        blist.mark(1, 1, Dragonstash::Blocklist::READ);
        blist.mark(2, 1, Dragonstash::Blocklist::READAHEAD);
        blist.mark(3, 1, Dragonstash::Blocklist::PINNED);
        blist.mark(5, 1, Dragonstash::Blocklist::WRITTEN);

        WHEN("Truncating access for an absent block") {
            THEN("truncate_access returns zero length") {
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE-1, 456) == 0);
                CHECK(blist.truncate_access(Dragonstash::CACHE_PAGE_SIZE*6, 456) == 0);
            }
        }

        WHEN("Truncating access starting at the block start and equal to block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE) ==
                      Dragonstash::CACHE_PAGE_SIZE);
            }
        }

        WHEN("Truncating access starting at the block start and with a length less than block size") {
            THEN("The original length is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE - 1) ==
                      Dragonstash::CACHE_PAGE_SIZE - 1);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE / 2) ==
                      Dragonstash::CACHE_PAGE_SIZE / 2);
            }
        }

        WHEN("Truncating access starting at the block start and with a length larger than block size but smaller than range length") {
            THEN("The original size is returned") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE + 1) ==
                      Dragonstash::CACHE_PAGE_SIZE+1);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE * 3) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
            }
        }

        WHEN("Truncating access starting at the block start and with a length larger than range length") {
            THEN("The size is truncated to the end of range") {
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE*3+1) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
                CHECK(blist.truncate_access(
                          Dragonstash::CACHE_PAGE_SIZE,
                          Dragonstash::CACHE_PAGE_SIZE * 4) ==
                      Dragonstash::CACHE_PAGE_SIZE*3);
            }
        }
    }

}

TEST_CASE("Split ranges exceeding limits into multiple ranges", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(0, (1<<18) - 15, Dragonstash::Blocklist::READ);

    CHECK(blist.state(1<<16) == Dragonstash::Blocklist::READ);
    CHECK(blist.state(1<<17) == Dragonstash::Blocklist::READ);
    CHECK(blist.state((1<<17) + (1<<16)) == Dragonstash::Blocklist::READ);
    CHECK(blist.state((1<<18) - 16) == Dragonstash::Blocklist::READ);

    CHECK(blist.nentries() == 4);
    CHECK(blist.blocks(Dragonstash::Blocklist::READ) == (1<<18) - 15);
}

TEST_CASE("Mark several ranges as absent with overlapping range",
          "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(0, 1, Dragonstash::Blocklist::READ);
    blist.mark(2, 1, Dragonstash::Blocklist::READ);
    blist.mark(4, 1, Dragonstash::Blocklist::READ);
    blist.mark(6, 1, Dragonstash::Blocklist::READ);

    CHECK(blist.nentries() == 4);

    blist.mark(0, 7, Dragonstash::Blocklist::ABSENT);

    CHECK(blist.nentries() == 0);
}

TEST_CASE("Change state of several ranges in single overlapping range",
          "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    blist.mark(0, 1, Dragonstash::Blocklist::READ);
    blist.mark(2, 1, Dragonstash::Blocklist::READ);
    blist.mark(4, 1, Dragonstash::Blocklist::READ);
    blist.mark(6, 1, Dragonstash::Blocklist::READ);

    CHECK(blist.nentries() == 4);

    blist.mark(0, 7, Dragonstash::Blocklist::READAHEAD);

    CHECK(blist.nentries() == 1);

    for (std::size_t i = 0; i < 7; ++i) {
        CHECK(blist.state(i) == Dragonstash::Blocklist::READAHEAD);
    }
    CHECK(blist.state(7) == Dragonstash::Blocklist::ABSENT);
}

TEST_CASE("Shrink after growth", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    auto initial_capacity = blist.capacity();

    for (decltype(initial_capacity) i = 0; i < initial_capacity + 1; ++i) {
        blist.mark(i * 2, 1, Dragonstash::Blocklist::READ);
    }

    CHECK(blist.capacity() > initial_capacity);
    CHECK(blist.nentries() == initial_capacity+1);

    // only removing half of the entries is already sufficient to allow
    // shrinking
    blist.mark(0, initial_capacity, Dragonstash::Blocklist::ABSENT);

    // no auto-shrink because it may be expensive
    CHECK(blist.capacity() > initial_capacity);

    blist.shrink();

    CHECK(blist.nentries() <= initial_capacity);
    CHECK(blist.capacity() == initial_capacity);
}

TEST_CASE("fsck shrinks", "[blocklist]")
{
    TestBlocklist env;
    Dragonstash::Blocklist &blist = env.blocklist();

    auto initial_capacity = blist.capacity();

    for (decltype(initial_capacity) i = 0; i < initial_capacity + 1; ++i) {
        blist.mark(i * 2, 1, Dragonstash::Blocklist::READ);
    }

    CHECK(blist.capacity() > initial_capacity);
    CHECK(blist.nentries() == initial_capacity+1);

    // only removing half of the entries is already sufficient to allow
    // shrinking
    blist.mark(0, initial_capacity, Dragonstash::Blocklist::ABSENT);
    blist.fsck();

    CHECK(blist.nentries() <= initial_capacity);
    CHECK(blist.capacity() == initial_capacity);
}
