#ifndef DRAGONSTASH_FUSE_BUFFER_H
#define DRAGONSTASH_FUSE_BUFFER_H

#include <cstring>
#include <string>

struct fuse_entry_param;
struct stat;

namespace Fuse {

class Request;

class DirBuffer {
private:
    std::basic_string<char> m_buf;

    size_t prepare_add(Request &req, const char *name);

public:
    void add(Request &req,
             const char *name,
             const struct stat &stbuf,
             const off_t off);
    void add(Request &req,
             const char *name,
             const struct stat &stbuf);

    inline const std::basic_string<char> &get() const {
        return m_buf;
    }
};


class DirBufferPlus {
private:
    std::basic_string<char> m_buf;

    size_t prepare_add(Request &req,
                       const char *name,
                       const fuse_entry_param &e);
    void execute_add(Request &req,
                     const size_t old_size,
                     const char *name,
                     const fuse_entry_param &e,
                     const off_t off);

public:
    void add(Request &req,
             const char *name,
             const fuse_entry_param &e,
             const off_t off);
    void add(Request &req,
             const char *name,
             const fuse_entry_param &e);

    inline const std::basic_string<char> &get() const {
        return m_buf;
    }
};

}

#endif
