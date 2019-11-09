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
             off_t off);
    void add(Request &req,
             const char *name,
             const struct stat &stbuf);

    [[nodiscard]] inline const std::basic_string<char> &get() const {
        return m_buf;
    }

    [[nodiscard]] inline std::size_t length() const {
        return m_buf.size();
    }
};


class DirBufferPlus {
private:
    std::basic_string<char> m_buf;

    size_t prepare_add(Request &req,
                       const char *name,
                       const fuse_entry_param &e);
    void execute_add(Request &req,
                     size_t old_size,
                     const char *name,
                     const fuse_entry_param &e,
                     off_t off);

public:
    void add(Request &req,
             const char *name,
             const fuse_entry_param &e,
             off_t off);
    void add(Request &req,
             const char *name,
             const fuse_entry_param &e);

    [[nodiscard]] inline const std::basic_string<char> &get() const {
        return m_buf;
    }

    [[nodiscard]] inline std::size_t length() const {
        return m_buf.size();
    }

    inline void rewind(std::size_t offs) {
        if (m_buf.size() > offs) {
            m_buf.resize(offs);
        }
    }
};

}

#endif
