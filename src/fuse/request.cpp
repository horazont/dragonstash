#include "fuse/request.hpp"

namespace Fuse {

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
