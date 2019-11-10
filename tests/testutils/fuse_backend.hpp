#ifndef DRAGONSTASH_TESTUTILS_FUSE_BACKEND_H
#define DRAGONSTASH_TESTUTILS_FUSE_BACKEND_H

#include <cstdint>
#include <optional>
#include <variant>
#include <tuple>

#include "fuse/request.hpp"


enum class TestFuseReplyType {
    NONE,
    ERROR,
    ENTRY,
    CREATE,
    ATTR,
    READLINK,
    OPEN,
    WRITE,
    BUF,
    DATA,
};


using TestFuseReplyNone = std::monostate;
using TestFuseReplyErr = int;
using TestFuseReplyEntry = fuse_entry_param;
using TestFuseReplyCreate = std::tuple<fuse_entry_param, fuse_file_info>;
using TestFuseReplyAttr = std::tuple<struct stat, double>;
using TestFuseReplyReadlink = std::string;
using TestFuseReplyOpen = fuse_file_info;
using TestFuseReplyWrite = size_t;
using TestFuseReplyBuf = TestFuseReplyReadlink;
using TestFuseReplyData = std::tuple<fuse_bufvec, fuse_buf_copy_flags>;
using TestFuseReplyVariant = std::variant<TestFuseReplyNone, TestFuseReplyErr, TestFuseReplyEntry, TestFuseReplyCreate, TestFuseReplyAttr, TestFuseReplyReadlink, TestFuseReplyOpen, TestFuseReplyWrite, TestFuseReplyData>;
using TestFuseReplyWrapper = std::tuple<TestFuseReplyType, TestFuseReplyVariant>;


class TestFuseRequest {
public:
    TestFuseRequest() = delete;
    TestFuseRequest(std::uint64_t id);

private:
    std::uint64_t m_id;
    std::optional<TestFuseReplyWrapper> m_reply;

public:
    inline operator Fuse::Request() {
        return Fuse::Request(reinterpret_cast<fuse_req_t>(this));
    }

    template <typename... Ts>
    void record_reply(TestFuseReplyType type, Ts&&...vs)
    {
        if (m_reply.has_value()) {
            throw std::logic_error("Reply sent twice to the same request.");
        }
        if constexpr (sizeof...(vs) == 0) {
            m_reply = std::make_tuple(type, std::monostate());
        } else if constexpr (sizeof...(vs) == 1) {
            m_reply = std::make_tuple(type, std::forward<Ts>(vs)...);
        } else {
            m_reply = std::make_tuple(type, std::make_tuple(std::forward<Ts>(vs)...));
        }
    }

    [[nodiscard]] bool has_reply() const {
        return m_reply.has_value();
    }

    [[nodiscard]] TestFuseReplyType reply_type() const {
        return std::get<0>(*m_reply);
    }

    [[nodiscard]] const TestFuseReplyVariant &reply_argv() const {
        return std::get<1>(*m_reply);
    }
};


class TestFuseBackend {
public:
    TestFuseBackend();
    TestFuseBackend(const TestFuseBackend &src) = delete;
    TestFuseBackend(TestFuseBackend &&src) = delete;
    TestFuseBackend &operator=(const TestFuseBackend &src) = delete;
    TestFuseBackend &operator=(TestFuseBackend &&src) = delete;
    ~TestFuseBackend();

private:
    Fuse::RequestBackend m_backup;
    std::uint64_t m_id_counter;

public:
    TestFuseRequest new_request();

};

#endif
