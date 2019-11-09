#ifndef DRAGONSTASH_FUSE_INTERFACE_H
#define DRAGONSTASH_FUSE_INTERFACE_H

#include "dragonstash-config.h"
#include "fuse_lowlevel.h"

#include <stdexcept>

#include "fuse/request.hpp"

namespace Fuse {

#define dragonstash_fuse_dispatch(func, ...) do {\
    Request request_handle(req); \
    Impl *impl = static_cast<Impl*>(request_handle.userdata()); \
    impl->func(std::move(request_handle), __VA_ARGS__); \
    } while (0)

template <typename Impl>
class Session {
public:
    Session(Impl &impl, struct fuse_args *args):
        m_impl(&impl),
        m_op({
             .init = &Session<Impl>::init,
             .destroy = &Session<Impl>::destroy,
             /* generate using:
              * sed -r 's/\s+void (\w+)\(Fuse::Request &&req, ([^)]+)\);/        .\1 = \&Session<Impl>::\1,/' < calls
              *
              * where `calls` containst the calls from Dragonstash::FuseInterface */
             .lookup = &Session<Impl>::lookup,
             .forget = &Session<Impl>::forget,
             .getattr = &Session<Impl>::getattr,
             .setattr = &Session<Impl>::setattr,
             .readlink = &Session<Impl>::readlink,
             .mknod = &Session<Impl>::mknod,
             .mkdir = &Session<Impl>::mkdir,
             .unlink = &Session<Impl>::unlink,
             .rmdir = &Session<Impl>::rmdir,
             .symlink = &Session<Impl>::symlink,
             .rename = &Session<Impl>::rename,
             .link = &Session<Impl>::link,
             .open = &Session<Impl>::open,
             .read = &Session<Impl>::read,
             .write = &Session<Impl>::write,
             .flush = &Session<Impl>::flush,
             .release = &Session<Impl>::release,
             .fsync = &Session<Impl>::fsync,
             .opendir = &Session<Impl>::opendir,
             .readdir = &Session<Impl>::readdir,
             .releasedir = &Session<Impl>::releasedir,
             .fsyncdir = &Session<Impl>::fsyncdir,
             .statfs = &Session<Impl>::statfs,
             .setxattr = &Session<Impl>::setxattr,
             .getxattr = &Session<Impl>::getxattr,
             .listxattr = &Session<Impl>::listxattr,
             .removexattr = &Session<Impl>::removexattr,
             .access = &Session<Impl>::access,
             .create = &Session<Impl>::create,
             .getlk = &Session<Impl>::getlk,
             .setlk = &Session<Impl>::setlk,
             .bmap = &Session<Impl>::bmap,
             .ioctl = &Session<Impl>::ioctl,
             .poll = &Session<Impl>::poll,
             .write_buf = &Session<Impl>::write_buf,
             .retrieve_reply = &Session<Impl>::retrieve_reply,
             .forget_multi = &Session<Impl>::forget_multi,
             .flock = &Session<Impl>::flock,
             .fallocate = &Session<Impl>::fallocate,
             .readdirplus = &Session<Impl>::readdirplus,
             .copy_file_range = &Session<Impl>::copy_file_range,
             }),
        m_session(fuse_session_new(args, &m_op, sizeof(m_op), m_impl))
    {
        if (!m_session) {
            throw std::runtime_error("failed to set up session");
        }

    }
    ~Session() {
        fuse_session_destroy(m_session);
    }

private:
    Impl *m_impl;
    fuse_lowlevel_ops m_op;
    struct fuse_session *m_session;

private:
    static void init(void *userdata, struct fuse_conn_info *conn) {
        Impl *impl = static_cast<Impl*>(userdata);
        impl->init(conn);
    }

    static void destroy(void *userdata) {
        Impl *impl = static_cast<Impl*>(userdata);
        impl->destroy();
    }

    /* generate using:
     * sed -r 's/\s+void (\w+)\(Fuse::Request &&req, ([^)]+)\);/    static void \1(fuse_req_t req, \2) {\n        dragonstash_fuse_dispatch(\1, \2);\n    }\n/' < calls | sed -r '/dragonstash/s/(unsigned |const |struct )*\w+\s([*&]?\w+)/\2/g;/dragonstash/s/\*(\w+)/\1/g;/dragonstash/s/&(\w+)/*\1/g;/static void/s/&(\w+)/*\1/g'
     *
     * where `calls` contains the list of calls from Dragonstash::FuseInterface */

    static void lookup(fuse_req_t req, fuse_ino_t parent, const char *name) {
        dragonstash_fuse_dispatch(lookup, parent, name);
    }

    static void forget(fuse_req_t req, fuse_ino_t ino, uint64_t nlookup) {
        dragonstash_fuse_dispatch(forget, ino, nlookup);
    }

    static void getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(getattr, ino, fi);
    }

    static void setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(setattr, ino, *attr, to_set, fi);
    }

    static void readlink(fuse_req_t req, fuse_ino_t ino) {
        dragonstash_fuse_dispatch(readlink, ino);
    }

    static void mknod(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, dev_t rdev) {
        dragonstash_fuse_dispatch(mknod, parent, name, mode, rdev);
    }

    static void mkdir(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode) {
        dragonstash_fuse_dispatch(mkdir, parent, name, mode);
    }

    static void unlink(fuse_req_t req, fuse_ino_t parent, const char *name) {
        dragonstash_fuse_dispatch(unlink, parent, name);
    }

    static void rmdir(fuse_req_t req, fuse_ino_t parent, const char *name) {
        dragonstash_fuse_dispatch(rmdir, parent, name);
    }

    static void symlink(fuse_req_t req, const char *link, fuse_ino_t parent, const char *name) {
        dragonstash_fuse_dispatch(symlink, link, parent, name);
    }

    static void rename(fuse_req_t req, fuse_ino_t parent, const char *name, fuse_ino_t newparent, const char *newname, unsigned int flags) {
        dragonstash_fuse_dispatch(rename, parent, name, newparent, newname, flags);
    }

    static void link(fuse_req_t req, fuse_ino_t ino, fuse_ino_t newparent, const char *name) {
        dragonstash_fuse_dispatch(link, ino, newparent, name);
    }

    static void open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(open, ino, fi);
    }

    static void read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(read, ino, size, off, fi);
    }

    static void write(fuse_req_t req, fuse_ino_t ino, const char *buf, size_t size, off_t off, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(write, ino, std::string_view(buf, size), off, fi);
    }

    static void flush(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(flush, ino, fi);
    }

    static void release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(release, ino, fi);
    }

    static void fsync(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(fsync, ino, datasync, fi);
    }

    static void opendir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(opendir, ino, fi);
    }

    static void readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(readdir, ino, size, off, fi);
    }

    static void releasedir(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(releasedir, ino, fi);
    }

    static void fsyncdir(fuse_req_t req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(fsyncdir, ino, datasync, fi);
    }

    static void statfs(fuse_req_t req, fuse_ino_t ino) {
        dragonstash_fuse_dispatch(statfs, ino);
    }

    static void setxattr(fuse_req_t req, fuse_ino_t ino, const char *name, const char *value, size_t size, int flags) {
        dragonstash_fuse_dispatch(setxattr, ino, name, std::string_view(value, size), flags);
    }

    static void getxattr(fuse_req_t req, fuse_ino_t ino, const char *name, size_t size) {
        dragonstash_fuse_dispatch(getxattr, ino, name, size);
    }

    static void listxattr(fuse_req_t req, fuse_ino_t ino, size_t size) {
        dragonstash_fuse_dispatch(listxattr, ino, size);
    }

    static void removexattr(fuse_req_t req, fuse_ino_t ino, const char *name) {
        dragonstash_fuse_dispatch(removexattr, ino, name);
    }

    static void access(fuse_req_t req, fuse_ino_t ino, int mask) {
        dragonstash_fuse_dispatch(access, ino, mask);
    }

    static void create(fuse_req_t req, fuse_ino_t parent, const char *name, mode_t mode, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(create, parent, name, mode, fi);
    }

    static void getlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
        dragonstash_fuse_dispatch(getlk, ino, fi, lock);
    }

    static void setlk(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep) {
        dragonstash_fuse_dispatch(setlk, ino, fi, lock, sleep);
    }

    static void bmap(fuse_req_t req, fuse_ino_t ino, size_t blocksize, uint64_t idx) {
        dragonstash_fuse_dispatch(bmap, ino, blocksize, idx);
    }

    static void ioctl(fuse_req_t req, fuse_ino_t ino, unsigned int cmd, void *arg, struct fuse_file_info *fi, unsigned flags, const void *in_buf, size_t in_bufsz, size_t out_bufsz) {
        dragonstash_fuse_dispatch(ioctl, ino, cmd, arg, fi, flags, in_buf, in_bufsz, out_bufsz);
    }

    static void poll(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, struct fuse_pollhandle *ph) {
        dragonstash_fuse_dispatch(poll, ino, fi, ph);
    }

    static void write_buf(fuse_req_t req, fuse_ino_t ino, struct fuse_bufvec *bufv, off_t offset, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(write_buf, ino, bufv, offset, fi);
    }

    static void retrieve_reply(fuse_req_t req, void *cookie, fuse_ino_t ino, off_t offset, struct fuse_bufvec *bufv) {
        dragonstash_fuse_dispatch(retrieve_reply, cookie, ino, offset, bufv);
    }

    static void forget_multi(fuse_req_t req, size_t count, struct fuse_forget_data *forgets) {
        dragonstash_fuse_dispatch(forget_multi, count, forgets);
    }

    static void flock(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi, int op) {
        dragonstash_fuse_dispatch(flock, ino, fi, op);
    }

    static void fallocate(fuse_req_t req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(fallocate, ino, mode, offset, length, fi);
    }

    static void readdirplus(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
        dragonstash_fuse_dispatch(readdirplus, ino, size, off, fi);
    }

    static void copy_file_range(fuse_req_t req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info *fi_in, fuse_ino_t ino_out, off_t off_out, struct fuse_file_info *fi_out, size_t len, int flags) {
        dragonstash_fuse_dispatch(copy_file_range, ino_in, off_in, fi_in, ino_out, off_out, fi_out, len, flags);
    }

public:
    inline int mount(const char *mountpoint) {
        return fuse_session_mount(m_session, mountpoint);
    }

    inline int loop() {
        return fuse_session_loop(m_session);
    }

    inline int loop_mt(bool clone_fds) {
        return fuse_session_loop_mt(m_session, clone_fds);
    }

    inline void unmount() {
        fuse_session_unmount(m_session);
    }

    inline int set_signal_handlers() {
        return fuse_set_signal_handlers(m_session);
    }

    inline void remove_signal_handlers() {
        fuse_remove_signal_handlers(m_session);
    }

};

#undef dragonstash_fuse_dispatch

class Interface {
public:
    void init(struct fuse_conn_info *conn);
    void destroy();
    void lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name);
    void forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup);
    void getattr(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void setattr(Fuse::Request &&req, fuse_ino_t ino, struct stat &attr, int to_set, struct fuse_file_info *fi);
    void readlink(Fuse::Request &&req, fuse_ino_t ino);
    void mknod(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode, dev_t rdev);
    void mkdir(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode);
    void unlink(Fuse::Request &&req, fuse_ino_t parent, std::string_view name);
    void rmdir(Fuse::Request &&req, fuse_ino_t parent, std::string_view name);
    void symlink(Fuse::Request &&req, const char *link, fuse_ino_t parent, std::string_view name);
    void rename(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, fuse_ino_t newparent, std::string_view newname, unsigned int flags);
    void link(Fuse::Request &&req, fuse_ino_t ino, fuse_ino_t newparent, std::string_view name);
    void open(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void read(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void write(Fuse::Request &&req, fuse_ino_t ino, std::string_view buf, off_t off, struct fuse_file_info *fi);
    void flush(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void release(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void fsync(Fuse::Request &&req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    void opendir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void readdir(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void releasedir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi);
    void fsyncdir(Fuse::Request &&req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi);
    void statfs(Fuse::Request &&req, fuse_ino_t ino);
    void setxattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name, std::string_view value, int flags);
    void getxattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name, size_t size);
    void listxattr(Fuse::Request &&req, fuse_ino_t ino, size_t size);
    void removexattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name);
    void access(Fuse::Request &&req, fuse_ino_t ino, int mask);
    void create(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode, struct fuse_file_info *fi);
    void getlk(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock);
    void setlk(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep);
    void bmap(Fuse::Request &&req, fuse_ino_t ino, size_t blocksize, uint64_t idx);
    void ioctl(Fuse::Request &&req, fuse_ino_t ino, unsigned int cmd, void *arg, struct fuse_file_info *fi, unsigned flags, const void *in_buf, size_t in_bufsz, size_t out_bufsz);
    void poll(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct fuse_pollhandle *ph);
    void write_buf(Fuse::Request &&req, fuse_ino_t ino, struct fuse_bufvec *bufv, off_t offset, struct fuse_file_info *fi);
    void retrieve_reply(Fuse::Request &&req, void *cookie, fuse_ino_t ino, off_t offset, struct fuse_bufvec *bufv);
    void forget_multi(Fuse::Request &&req, size_t count, struct fuse_forget_data *forgets);
    void flock(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, int op);
    void fallocate(Fuse::Request &&req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi);
    void readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi);
    void copy_file_range(Fuse::Request &&req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info *fi_in, fuse_ino_t ino_out, off_t off_out, struct fuse_file_info *fi_out, size_t len, int flags);
};

}

#endif
