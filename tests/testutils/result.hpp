/**********************************************************************
File name: result.hpp
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
#ifndef DRAGONSTASH_TESTS_TESTUTILS_ERROR_H
#define DRAGONSTASH_TESTS_TESTUTILS_ERROR_H

#include <catch2/catch.hpp>

// These must be macros to get useful line numbers in Catch2 output.

#define check_result_error(result, err) do { \
    const auto &result_tmp ## __LINE__ = (result); \
    CHECK(!result_tmp ## __LINE__); \
    CHECK((result_tmp ## __LINE__).error() == (err)); \
    } while (0);

#define require_result_ok(result) do { \
    const auto &result_tmp ## __LINE__ = (result); \
    CHECK((result_tmp ## __LINE__).error() == 0); \
    REQUIRE(result_tmp ## __LINE__); \
    } while (0);

#define check_result_ok(result) do { \
    const auto &result_tmp ## __LINE__ = (result); \
    CHECK((result_tmp ## __LINE__).error() == 0); \
    CHECK(result_tmp ## __LINE__); \
    } while (0);

#endif
