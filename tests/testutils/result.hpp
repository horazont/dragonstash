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
