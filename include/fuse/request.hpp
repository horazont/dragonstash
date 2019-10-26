#ifndef DRAGONSTASH_FUSE_REQUEST_H
#define DRAGONSTASH_FUSE_REQUEST_H

#include "dragonstash-config.h"
#include "fuse_lowlevel.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

namespace Fuse {

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
        return fuse_req_userdata(m_req);
    }

    inline const fuse_ctx *ctx() {
        return fuse_req_ctx(m_req);
    }

    inline int getgroups(int size, gid_t *list) {
        return fuse_req_getgroups(m_req, size, list);
    }

    inline void interrupt_func(fuse_interrupt_func_t func,
                               void *data)
    {
        return fuse_req_interrupt_func(m_req, func, data);
    }

    inline int interrupted() {
        return fuse_req_interrupted(m_req);
    }

public: /* REPLY FUNCTIONS */
    inline void reply_none() {
        fuse_reply_none(release());
    }

    inline int reply_err(int err)
    {
        check();
        return fuse_reply_err(release(), err);
    }

    inline int reply_entry(const fuse_entry_param *e)
    {
        check();
        return fuse_reply_entry(release(), e);
    }

    inline int reply_create(const fuse_entry_param *e,
                            const fuse_file_info *fi)
    {
        check();
        return fuse_reply_create(release(), e, fi);
    }

    inline int reply_attr(const struct stat &attr, double attr_timeout)
    {
        check();
        return fuse_reply_attr(release(), &attr, attr_timeout);
    }

    inline int reply_readlink(const char *link) {
        check();
        return fuse_reply_readlink(release(), link);
    }

    inline int reply_open(const fuse_file_info *fi) {
        check();
        return fuse_reply_open(release(), fi);
    }

    inline int reply_write(size_t count) {
        check();
        return fuse_reply_write(release(), count);
    }

    inline int reply_buf(const char *buf, size_t size) {
        check();
        return fuse_reply_buf(release(), buf, size);
    }

    inline int reply_buf(const std::basic_string<char> &s) {
        check();
        return reply_buf(s.data(), s.size());
    }

    inline int reply_data(struct fuse_bufvec *bufv, enum fuse_buf_copy_flags flags) {
        check();
        return fuse_reply_data(release(), bufv, flags);
    }

    inline int reply_iov(const iovec *iov, int count) {
        check();
        return fuse_reply_iov(release(), iov, count);
    }

    inline int reply_statfs(const struct statvfs *stbuf) {
        check();
        return fuse_reply_statfs(release(), stbuf);
    }

    inline int reply_xattr(size_t count) {
        check();
        return fuse_reply_xattr(release(), count);
    }

    inline int reply_lock(const flock *lock) {
        check();
        return fuse_reply_lock(release(), lock);
    }

    inline int reply_bmap(uint64_t idx) {
        check();
        return fuse_reply_bmap(release(), idx);
    }

    inline int reply_ioctl_retry(const iovec *in_iov, size_t in_count,
                                 const iovec *out_iov, size_t out_count) {
        check();
        return fuse_reply_ioctl_retry(release(),
                                      in_iov, in_count,
                                      out_iov, out_count);
    }

    inline int reply_ioctl(int result, const void *buf, size_t size) {
        check();
        return fuse_reply_ioctl(release(), result, buf, size);
    }

    inline int reply_ioctl_iov(int result, const iovec *iov, int count) {
        check();
        return fuse_reply_ioctl_iov(release(), result, iov, count);
    }

    inline int reply_poll(unsigned revents) {
        check();
        return fuse_reply_poll(release(), revents);
    }
};
}

#endif
