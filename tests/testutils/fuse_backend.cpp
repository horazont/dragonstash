/**********************************************************************
File name: fuse_backend.cpp
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
#include "fuse_backend.hpp"


static TestFuseRequest &get_impl(fuse_req_t req)
{
    return *reinterpret_cast<TestFuseRequest*>(req);
}

static void *dummy_req_userdata(fuse_req_t req)
{
    return &get_impl(req);
}

static const fuse_ctx *dummy_req_ctx(fuse_req_t)
{
    return nullptr;
}

[[noreturn]] static void not_implemented()
{
    throw std::runtime_error("not implemented");
}

static int dummy_req_getgroups(fuse_req_t, int, gid_t*)
{
    not_implemented();
}

static void dummy_req_interrupt_func(fuse_req_t, fuse_interrupt_func_t, void*)
{
    not_implemented();
}

static int dummy_req_interrupted(fuse_req_t)
{
    not_implemented();
}

static void dummy_reply_none(fuse_req_t req)
{
    get_impl(req).record_reply(TestFuseReplyType::NONE);
}

static int dummy_reply_err(fuse_req_t req, int error)
{
    get_impl(req).record_reply(TestFuseReplyType::ERROR, error);
    return 0;
}

static int dummy_reply_entry(fuse_req_t req, const fuse_entry_param *e)
{
    get_impl(req).record_reply(TestFuseReplyType::ENTRY, *e);
    return 0;
}

static int dummy_reply_create(fuse_req_t req,
                              const fuse_entry_param *e,
                              const fuse_file_info *fi)
{
    get_impl(req).record_reply(TestFuseReplyType::CREATE, *e, *fi);
    return 0;
}

static int dummy_reply_attr(fuse_req_t req, const struct stat *attr,
                            double timeout)
{
    get_impl(req).record_reply(TestFuseReplyType::ATTR, *attr, timeout);
    return 0;
}

static int dummy_reply_readlink(fuse_req_t req, const char *link)
{
    get_impl(req).record_reply(TestFuseReplyType::READLINK, std::string(link));
    return 0;
}

static int dummy_reply_open(fuse_req_t req, const fuse_file_info *fi)
{
    get_impl(req).record_reply(TestFuseReplyType::OPEN, *fi);
    return 0;
}

static int dummy_reply_write(fuse_req_t req, size_t sz)
{
    get_impl(req).record_reply(TestFuseReplyType::WRITE, sz);
    return 0;
}

static int dummy_reply_buf(fuse_req_t req, const char *data, size_t sz)
{
    get_impl(req).record_reply(TestFuseReplyType::BUF, std::string(data, sz));
    return 0;
}

static int dummy_reply_data(fuse_req_t req, struct fuse_bufvec *bv,
                            enum fuse_buf_copy_flags flags)
{
    get_impl(req).record_reply(TestFuseReplyType::DATA, *bv, flags);
    return 0;
}


TestFuseRequest::TestFuseRequest(uint64_t id):
    m_id(id)
{

}


TestFuseBackend::TestFuseBackend():
    m_backup(Fuse::backend),
    m_id_counter(1)
{
    Fuse::backend.req_userdata = &dummy_req_userdata;
    Fuse::backend.req_ctx = &dummy_req_ctx;
    Fuse::backend.req_getgroups = &dummy_req_getgroups;
    Fuse::backend.req_interrupt_func = &dummy_req_interrupt_func;
    Fuse::backend.req_interrupted = &dummy_req_interrupted;

    Fuse::backend.reply_none = &dummy_reply_none;
    Fuse::backend.reply_err = &dummy_reply_err;
    Fuse::backend.reply_entry = &dummy_reply_entry;
    Fuse::backend.reply_create = &dummy_reply_create;
    Fuse::backend.reply_attr = &dummy_reply_attr;
    Fuse::backend.reply_readlink = &dummy_reply_readlink;
    Fuse::backend.reply_open = &dummy_reply_open;
    Fuse::backend.reply_write = &dummy_reply_write;
    Fuse::backend.reply_buf = &dummy_reply_buf;
    Fuse::backend.reply_data = &dummy_reply_data;
}

TestFuseBackend::~TestFuseBackend()
{
    Fuse::backend = m_backup;
}

TestFuseRequest TestFuseBackend::new_request()
{
    return TestFuseRequest(m_id_counter++);
}


#include <catch2/catch.hpp>

// self-tests

TEST_CASE("Reply interception", "[selftest]")
{
    TestFuseBackend fake_backend;
    auto req_wrap = fake_backend.new_request();
    auto req = Fuse::Request(req_wrap);

    CHECK(!req_wrap.has_reply());

    SECTION("reply_none") {
        req.reply_none();

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::NONE);
        CHECK(std::holds_alternative<TestFuseReplyNone>(req_wrap.reply_argv()));
    }

    SECTION("reply_err") {
        req.reply_err(1234);

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::ERROR);
        REQUIRE(std::holds_alternative<TestFuseReplyErr>(req_wrap.reply_argv()));
        CHECK(std::get<TestFuseReplyErr>(req_wrap.reply_argv()) == 1234);
    }

    SECTION("reply_entry") {
        struct fuse_entry_param e{};
        e.ino = 1;
        e.attr_timeout = 1.0;
        e.entry_timeout = 2.0;

        req.reply_entry(&e);
        e.ino = 2;

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::ENTRY);
        REQUIRE(std::holds_alternative<TestFuseReplyEntry>(req_wrap.reply_argv()));

        auto entry = std::get<TestFuseReplyEntry>(req_wrap.reply_argv());
        CHECK(entry.ino == 1);
        CHECK(entry.attr_timeout == 1.0);
        CHECK(entry.entry_timeout == 2.0);
    }

    SECTION("reply_create") {
        struct fuse_entry_param e{};
        e.ino = 1;
        e.attr_timeout = 1.0;
        e.entry_timeout = 2.0;

        struct fuse_file_info fi{};
        fi.flags = 1234;

        req.reply_create(&e, &fi);
        e.ino = 2;
        fi.flags = 0;

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::CREATE);
        REQUIRE(std::holds_alternative<TestFuseReplyCreate>(req_wrap.reply_argv()));

        struct fuse_entry_param captured_e{};
        struct fuse_file_info captured_fi{};
        std::tie(captured_e, captured_fi) = std::get<TestFuseReplyCreate>(req_wrap.reply_argv());

        CHECK(captured_e.ino == 1);
        CHECK(captured_fi.flags == 1234);
    }

    SECTION("reply_attr") {
        struct stat attr{};
        attr.st_dev = 1234;
        attr.st_ino = 4567;

        req.reply_attr(attr, 2.0);
        attr.st_dev = 0;

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::ATTR);
        REQUIRE(std::holds_alternative<TestFuseReplyAttr>(req_wrap.reply_argv()));

        struct stat captured_attr{};
        double captured_timeout;
        std::tie(captured_attr, captured_timeout) = std::get<TestFuseReplyAttr>(req_wrap.reply_argv());

        CHECK(captured_attr.st_dev == 1234);
        CHECK(captured_attr.st_ino == 4567);
        CHECK(captured_timeout == 2.0);
    }

    SECTION("reply_readlink") {
        std::string link = "some link";

        req.reply_readlink(link.c_str());
        link[2] = 'x';

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::READLINK);
        REQUIRE(std::holds_alternative<TestFuseReplyReadlink>(req_wrap.reply_argv()));
        CHECK(std::get<TestFuseReplyReadlink>(req_wrap.reply_argv()) == "some link");
    }

    SECTION("reply_open") {
        struct fuse_file_info fi{};
        fi.flags = 1234;

        req.reply_open(&fi);
        fi.flags = 0;

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::OPEN);
        REQUIRE(std::holds_alternative<TestFuseReplyOpen>(req_wrap.reply_argv()));

        auto captured_fi = std::get<TestFuseReplyOpen>(req_wrap.reply_argv());
        CHECK(captured_fi.flags == 1234);
    }

    SECTION("reply_write") {
        req.reply_write(1234);

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::WRITE);
        REQUIRE(std::holds_alternative<TestFuseReplyWrite>(req_wrap.reply_argv()));
        CHECK(std::get<TestFuseReplyWrite>(req_wrap.reply_argv()) == 1234);
    }

    SECTION("reply_buf") {
        std::string buffer("foo bar baz");
        buffer.append(1, '\0');
        buffer.append("fnord");
        std::string copy(buffer);

        req.reply_buf(buffer);
        buffer[1] = 'x';

        REQUIRE(req_wrap.has_reply());
        CHECK(req_wrap.reply_type() == TestFuseReplyType::BUF);
        REQUIRE(std::holds_alternative<TestFuseReplyBuf>(req_wrap.reply_argv()));
        CHECK(std::get<TestFuseReplyBuf>(req_wrap.reply_argv()) == copy);
    }
}
