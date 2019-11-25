/**********************************************************************
File name: buffer.hpp
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
