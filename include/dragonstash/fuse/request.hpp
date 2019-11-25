/**********************************************************************
File name: request.hpp
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
#ifndef DRAGONSTASH_FUSE_REQUEST_H
#define DRAGONSTASH_FUSE_REQUEST_H

#include "dragonstash/dragonstash-config.h"
#include <fuse_lowlevel.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

namespace Fuse {

struct RequestBackend {
    void *(*req_userdata)(fuse_req_t);
    const fuse_ctx*(*req_ctx)(fuse_req_t);
    int (*req_getgroups)(fuse_req_t, int, gid_t*);
    void (*req_interrupt_func)(fuse_req_t, fuse_interrupt_func_t, void*);
    int (*req_interrupted)(fuse_req_t);

    void (*reply_none)(fuse_req_t);
    int (*reply_err)(fuse_req_t, int);
    int (*reply_entry)(fuse_req_t, const fuse_entry_param*);
    int (*reply_create)(fuse_req_t, const fuse_entry_param*,
                        const fuse_file_info*);
    int (*reply_attr)(fuse_req_t, const struct stat*, double);
    int (*reply_readlink)(fuse_req_t, const char*);
    int (*reply_open)(fuse_req_t, const fuse_file_info*);
    int (*reply_write)(fuse_req_t, size_t);
    int (*reply_buf)(fuse_req_t, const char*, size_t);
    int (*reply_data)(fuse_req_t, struct fuse_bufvec*,
                      enum fuse_buf_copy_flags);
    int (*reply_iov)(fuse_req_t, const iovec*, int);
    int (*reply_statfs)(fuse_req_t, const struct statvfs*);
    int (*reply_xattr)(fuse_req_t, size_t);
    int (*reply_lock)(fuse_req_t, const flock*);
    int (*reply_bmap)(fuse_req_t, uint64_t idx);
    int (*reply_ioctl_retry)(fuse_req_t,
                             const iovec*, size_t,
                             const iovec*, size_t);
    int (*reply_ioctl)(fuse_req_t, int, const void*, size_t);
    int (*reply_ioctl_iov)(fuse_req_t, int, const iovec*, int);
    int (*reply_poll)(fuse_req_t, unsigned);
};

extern RequestBackend backend;

class Request {
public:
    Request();
    explicit Request(fuse_req_t req, int default_error = ECANCELED);
    Request(const Request &src) = delete;
    Request(Request &&src) noexcept;
    Request &operator=(const Request &src) = delete;
    Request &operator=(Request &&src) noexcept;
    Request &operator=(std::nullptr_t);
    ~Request();

private:
    fuse_req_t m_req;
    int m_default_error;

protected:
    void reset();

    inline void check() {
#ifndef NDEBUG
        if (!(*this)) {
            throw std::runtime_error("attempt to execute an operation on a closed FuseRequest");
        }
#endif
    }

public:
    inline operator bool() const {
        return m_req != nullptr;
    }

    inline fuse_req_t release() {
        fuse_req_t result = m_req;
        m_req = nullptr;
        return result;
    }

    inline void swap(Request &other) {
        fuse_req_t tmp = m_req;
        m_req = other.m_req;
        other.m_req = tmp;
    }

    inline fuse_req_t operator*() const {
        return m_req;
    }

    inline fuse_req_t operator*() {
        return m_req;
    }

public: /* GETTERS */
    inline void *userdata() {
        return backend.req_userdata(m_req);
    }

    inline const fuse_ctx *ctx() {
        return backend.req_ctx(m_req);
    }

    inline int getgroups(int size, gid_t *list) {
        return backend.req_getgroups(m_req, size, list);
    }

    inline void interrupt_func(fuse_interrupt_func_t func,
                               void *data)
    {
        return backend.req_interrupt_func(m_req, func, data);
    }

    inline int interrupted() {
        return backend.req_interrupted(m_req);
    }

public: /* REPLY FUNCTIONS */
    inline void reply_none() {
        backend.reply_none(release());
    }

    inline int reply_err(int err)
    {
        check();
        return backend.reply_err(release(), err);
    }

    inline int reply_entry(const fuse_entry_param *e)
    {
        check();
        return backend.reply_entry(release(), e);
    }

    inline int reply_create(const fuse_entry_param *e,
                            const fuse_file_info *fi)
    {
        check();
        return backend.reply_create(release(), e, fi);
    }

    inline int reply_attr(const struct stat &attr, double attr_timeout)
    {
        check();
        return backend.reply_attr(release(), &attr, attr_timeout);
    }

    inline int reply_readlink(const char *link) {
        check();
        return backend.reply_readlink(release(), link);
    }

    inline int reply_open(const fuse_file_info *fi) {
        check();
        return backend.reply_open(release(), fi);
    }

    inline int reply_write(size_t count) {
        check();
        return backend.reply_write(release(), count);
    }

    inline int reply_buf(const char *buf, size_t size) {
        check();
        return backend.reply_buf(release(), buf, size);
    }

    inline int reply_buf(const std::basic_string<char> &s) {
        check();
        return reply_buf(s.data(), s.size());
    }

    inline int reply_data(struct fuse_bufvec *bufv, enum fuse_buf_copy_flags flags) {
        check();
        return backend.reply_data(release(), bufv, flags);
    }

    inline int reply_iov(const iovec *iov, int count) {
        check();
        return backend.reply_iov(release(), iov, count);
    }

    inline int reply_statfs(const struct statvfs *stbuf) {
        check();
        return backend.reply_statfs(release(), stbuf);
    }

    inline int reply_xattr(size_t count) {
        check();
        return backend.reply_xattr(release(), count);
    }

    inline int reply_lock(const flock *lock) {
        check();
        return backend.reply_lock(release(), lock);
    }

    inline int reply_bmap(uint64_t idx) {
        check();
        return backend.reply_bmap(release(), idx);
    }

    inline int reply_ioctl_retry(const iovec *in_iov, size_t in_count,
                                 const iovec *out_iov, size_t out_count) {
        check();
        return backend.reply_ioctl_retry(release(),
                                         in_iov, in_count,
                                         out_iov, out_count);
    }

    inline int reply_ioctl(int result, const void *buf, size_t size) {
        check();
        return backend.reply_ioctl(release(), result, buf, size);
    }

    inline int reply_ioctl_iov(int result, const iovec *iov, int count) {
        check();
        return backend.reply_ioctl_iov(release(), result, iov, count);
    }

    inline int reply_poll(unsigned revents) {
        check();
        return backend.reply_poll(release(), revents);
    }
};

}

#endif
