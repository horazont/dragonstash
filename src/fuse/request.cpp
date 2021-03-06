/**********************************************************************
File name: request.cpp
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
#include "dragonstash/fuse/request.hpp"

namespace Fuse {

RequestBackend backend{
    .req_userdata = &fuse_req_userdata,
    .req_ctx = &fuse_req_ctx,
    .req_getgroups = &fuse_req_getgroups,
    .req_interrupt_func = &fuse_req_interrupt_func,
    .req_interrupted = &fuse_req_interrupted,

    .reply_none = &fuse_reply_none,
    .reply_err = &fuse_reply_err,
    .reply_entry = &fuse_reply_entry,
    .reply_create = &fuse_reply_create,
    .reply_attr = &fuse_reply_attr,
    .reply_readlink = &fuse_reply_readlink,
    .reply_open = &fuse_reply_open,
    .reply_write = &fuse_reply_write,
    .reply_buf = &fuse_reply_buf,
    .reply_data = &fuse_reply_data,
    .reply_iov = &fuse_reply_iov,
    .reply_statfs = &fuse_reply_statfs,
    .reply_xattr = &fuse_reply_xattr,
    .reply_lock = &fuse_reply_lock,
    .reply_bmap = &fuse_reply_bmap,
    .reply_ioctl_retry = &fuse_reply_ioctl_retry,
    .reply_ioctl = &fuse_reply_ioctl,
    .reply_ioctl_iov = &fuse_reply_ioctl_iov,
    .reply_poll = &fuse_reply_poll,
};


Request::Request():
    m_req(nullptr),
    m_default_error(ECANCELED)
{

}

Request::Request(fuse_req_t req, int default_error):
    m_req(req),
    m_default_error(default_error)
{

}

Request::Request(Request &&src) noexcept:
    m_req(src.m_req),
    m_default_error(src.m_default_error)
{
    src.m_req = nullptr;
}

Request &Request::operator=(Request &&src) noexcept
{
    if (m_req) {
        reset();
    }
    m_req = src.m_req;
    m_default_error = src.m_default_error;
    src.m_req = nullptr;
    return *this;
}

Request &Request::operator=(std::nullptr_t)
{
    if (m_req) {
        reset();
    }
    m_req = nullptr;
    return *this;
}

Request::~Request()
{
    if (m_req) {
        reset();
    }
}

void Request::reset()
{
    reply_err(m_default_error);
}

}
