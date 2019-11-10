/**********************************************************************
File name: buffer.cpp
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
#include "fuse/buffer.hpp"

#include "fuse/request.hpp"

namespace Fuse {

size_t DirBuffer::prepare_add(Request &req, const char *name)
{
    const size_t old_size = m_buf.size();
    const size_t new_size = old_size + fuse_add_direntry(*req, nullptr, 0,
                                                         name, nullptr, 0);
    m_buf.resize(new_size);
    return old_size;
}

void DirBuffer::add(Request &req, const char *name, const struct stat &stbuf, const off_t off)
{
    const size_t old_size = prepare_add(req, name);
    fuse_add_direntry(*req, &m_buf[old_size], m_buf.size() - old_size, name, &stbuf, off);
}

void DirBuffer::add(Request &req,
                    const char *name,
                    const struct stat &stbuf)
{
    const size_t old_size = prepare_add(req, name);
    fuse_add_direntry(*req, &m_buf[old_size], m_buf.size() - old_size, name, &stbuf, m_buf.size());
}

size_t DirBufferPlus::prepare_add(Request &req,
                                  const char *name,
                                  const fuse_entry_param &e)
{
    const size_t old_size = m_buf.size();
    const size_t new_size = old_size + fuse_add_direntry_plus(
                *req, nullptr, 0,
                name, &e, 0);
    m_buf.resize(new_size);
    return old_size;
}

void DirBufferPlus::execute_add(Request &req,
                                const size_t old_size,
                                const char *name,
                                const fuse_entry_param &e, const off_t off)
{
    fuse_add_direntry_plus(*req, &m_buf[old_size], m_buf.size() - old_size,
                           name, &e, off);
}

void DirBufferPlus::add(Request &req, const char *name,
                        const fuse_entry_param &e,
                        const off_t off)
{
    const size_t old_size = prepare_add(req, name, e);
    execute_add(req, old_size, name, e, off);
}

void DirBufferPlus::add(Request &req, const char *name, const fuse_entry_param &e)
{
    const size_t old_size = prepare_add(req, name, e);
    execute_add(req, old_size, name, e, m_buf.size());
}

}
