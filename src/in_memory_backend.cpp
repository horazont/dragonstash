/**********************************************************************
File name: in_memory_backend.cpp
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
#include "dragonstash/in_memory_backend.hpp"

#include <cassert>
#include <cstring>

#include <sys/stat.h>
#include <sys/types.h>

namespace Dragonstash::Backend {

namespace InMemory {

Node::Node(const Stat &attr):
    m_attr(attr)
{

}

void Node::update_attr(const Stat &new_attr)
{
    auto mode_backup = m_attr.mode;
    m_attr = new_attr;
    m_attr.mode = (mode_backup & S_IFMT) | (m_attr.mode & ~S_IFMT);
}

Result<Node *> Node::find(std::string_view)
{
    return make_result(FAILED, ENOTDIR);
}

Node::~Node() = default;



File::File():
    Node(Stat{
         .mode = S_IFREG,
    })
{

}

File::~File() = default;


Link::Link():
    Link("")
{

}

Link::Link(std::string_view destination):
    Node(Stat{
         .mode = S_IFLNK
    }),
    m_destination(destination)
{

}

Link::~Link() = default;


Directory::Directory():
    Node(Stat{
         .mode = S_IFDIR
    })
{

}

Directory::~Directory() = default;

Result<Node*> Directory::find(std::string_view path)
{
    if (path.empty()) {
        return make_result(FAILED, EINVAL);
    }

    if (path[0] != '/') {
        return make_result(FAILED, EINVAL);
    }
    path.remove_prefix(1);

    if (path.empty()) {
        return this;
    }

    auto next_slash = path.find_first_of('/');
    std::string_view child_name;
    std::string_view remainder;
    if (next_slash == std::string_view::npos) {
        child_name = path;
    } else if (next_slash == 0) {
        return make_result(FAILED, EINVAL);
    } else {
        child_name = path.substr(0, next_slash);
        remainder = path.substr(next_slash);
        assert(!remainder.empty());
        assert(remainder[0] == '/');
    }

    auto child_node_iter = m_children.find(std::string(child_name));
    if (child_node_iter == m_children.end()) {
        return make_result(FAILED, ENOENT);
    }

    if (remainder.empty()) {
        return child_node_iter->second.get();
    }

    return child_node_iter->second->find(remainder);
}

void Directory::remove(const std::string &name)
{
    auto iter = m_children.find(name);
    if (iter == m_children.end()) {
        return;
    }

    m_children.erase(iter);
}

DirHandle::DirHandle(Directory &node):
    m_node(&node),
    m_state(DOT),
    m_iter(m_node->children().begin())
{

}

Result<DirEntry> DirHandle::readdir()
{
    switch (m_state) {
    case DOT:
        m_state = DOTDOT;
        return DirEntry{
            Stat{
                .ino = 0,
            },
            ".",
            false,
        };
    case DOTDOT:
        m_state = ITERATION;
        return DirEntry{
            Stat{
                .ino = 0,
            },
            "..",
            false,
        };
    case ITERATION:;
    }

    if (m_iter == m_node->children().end()) {
        return make_result(FAILED, 0);
    }

    auto iter = m_iter;
    ++m_iter;

    // TODO: allow complete dir entry results, because SFTP will return those.
    return DirEntry{
        Stat{
            .ino = 0,
        },
        iter->first,
        false,
    };
}

Result<void> DirHandle::fsyncdir()
{
    return make_result(FAILED, EOPNOTSUPP);
}

Result<void> DirHandle::closedir()
{
    return make_result(FAILED, EOPNOTSUPP);
}

DirHandle::~DirHandle() = default;

FileHandle::FileHandle(InMemory::File &file):
    m_file(&file)
{

}

FileHandle::~FileHandle() = default;

Result<Stat> FileHandle::fstat()
{
    return m_file->attr();
}

Result<ssize_t> FileHandle::pread(void *buf, size_t count, off_t offset)
{
    if (offset < 0) {
        return make_result(FAILED, EINVAL);
    }
    auto &data = m_file->data();
    if (static_cast<size_t>(offset) >= data.size()) {
        return 0;
    }
    const std::size_t end_offset = offset + count;
    if (end_offset > data.size()) {
        count -= (end_offset - data.size());
    }
    memcpy(buf, &data[offset], count);
    return make_result(count);
}

Result<ssize_t> FileHandle::pwrite(const void *buf, size_t count, off_t offset)
{
    if (offset < 0) {
        return make_result(FAILED, EINVAL);
    }
    const std::size_t required_size = offset + count;
    auto &data = m_file->data();
    if (data.size() < required_size) {
        data.resize(required_size);
        m_file->attr().size = required_size;
    }
    memcpy(&data[offset], buf, count);
    return make_result(count);
}

Result<void> FileHandle::fsync()
{
    return make_result();
}

Result<void> FileHandle::close()
{
    return make_result();
}

}

void InMemoryFilesystem::set_connected(bool connected)
{
    m_connected = connected;
}

Result<std::unique_ptr<File> > InMemoryFilesystem::open(std::string_view path, int accesstype, mode_t mode)
{
    if (!m_connected) {
        return make_result(FAILED, ENOTCONN);
    }

    Result<InMemory::Node*> node = find(path);
    if (!node) {
        return copy_error(node);
    }
    {
        auto *dir = dynamic_cast<InMemory::Directory*>(*node);
        if (dir) {
            return make_result(FAILED, EISDIR);
        }
    }
    auto *file = dynamic_cast<InMemory::File*>(*node);
    if (!file) {
        return make_result(FAILED, EINVAL);
    }
    return std::make_unique<InMemory::FileHandle>(*file);
}

Result<std::unique_ptr<Dir> > InMemoryFilesystem::opendir(std::string_view path)
{
    if (!m_connected) {
        return make_result(FAILED, ENOTCONN);
    }

    Result<InMemory::Node*> node = find(path);
    if (!node) {
        return copy_error(node);
    }
    auto *dir = dynamic_cast<InMemory::Directory*>(*node);
    if (!dir) {
        return make_result(FAILED, ENOTDIR);
    }
    return std::make_unique<InMemory::DirHandle>(*dir);
}

Result<Stat> InMemoryFilesystem::lstat(std::string_view path)
{
    if (!m_connected) {
        return make_result(FAILED, ENOTCONN);
    }

    Result<InMemory::Node*> node = find(path);
    if (!node) {
        return copy_error(node);
    }
    return (*node)->attr();
}

Result<std::string> InMemoryFilesystem::readlink(std::string_view path)
{
    if (!m_connected) {
        return make_result(FAILED, ENOTCONN);
    }

    Result<InMemory::Node*> node = find(path);
    if (!node) {
        return copy_error(node);
    }
    auto *link = dynamic_cast<InMemory::Link*>(*node);
    if (!link) {
        return make_result(FAILED, EINVAL);
    }
    return link->destination();
}

}
