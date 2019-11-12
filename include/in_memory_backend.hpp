/**********************************************************************
File name: in_memory_backend.hpp
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
#ifndef DRAGONSTASH_BACKEND_IN_MEMORY_H
#define DRAGONSTASH_BACKEND_IN_MEMORY_H

#include <memory>
#include <string>
#include <unordered_map>

#include "backend.hpp"

namespace Dragonstash::Backend {

namespace InMemory {

class Node {
public:
    Node() = default;
    explicit Node(const Stat &attr);
    Node(const Node &src) = delete;
    Node(Node &&src) = delete;
    Node &operator=(const Node &src) = delete;
    Node &operator=(Node &&src) = delete;
    virtual ~Node();

private:
    Stat m_attr{};

public:
    [[nodiscard]] Stat &attr() {
        return m_attr;
    }

    void update_attr(const Stat &new_attr);

    virtual Result<Node*> find(std::string_view path);

};

class File: public Node {
public:
    File();
    File(const File &src) = delete;
    File(File &&src) = delete;
    File &operator=(const File &src) = delete;
    File &operator=(File &&src) = delete;
    ~File() override;

private:
    std::basic_string<std::byte> m_data;

public:
    [[nodiscard]] std::basic_string<std::byte> &data() {
        return m_data;
    }

};

class FileHandle: public Dragonstash::Backend::File {
public:
    FileHandle() = delete;
    explicit FileHandle(InMemory::File &file);
    ~FileHandle() override;

private:
    InMemory::File *m_file;

    // File interface
public:
    Result<Stat> fstat() override;
    Result<ssize_t> pread(void *buf, size_t count, off_t offset) override;
    Result<ssize_t> pwrite(const void *buf, size_t count, off_t offset) override;
    Result<void> fsync() override;
    Result<void> close() override;
};

class Link: public Node {
public:
    Link();
    explicit Link(std::string_view destination);
    Link(const Link &src) = delete;
    Link(Link &&src) = delete;
    Link &operator=(const Link &src) = delete;
    Link &operator=(Link &&src) = delete;
    ~Link() override;

private:
    std::string m_destination;

public:
    [[nodiscard]] std::string &destination() {
        return m_destination;
    }

};

class Directory: public Node {
public:
    using Children = std::unordered_map<std::string, std::unique_ptr<Node>>;

public:
    Directory();
    Directory(const Directory &src) = delete;
    Directory(Directory &&src) = delete;
    Directory &operator=(const Directory &src) = delete;
    Directory &operator=(Directory &&src) = delete;
    ~Directory() override;

private:
    Children m_children;

public:
    [[nodiscard]] Children &children() {
        return m_children;
    }

    template<typename T, typename... Args>
    T &emplace(std::string_view name, Args&&... args)
    {
        auto ptr = std::make_unique<T>(std::forward<Args>(args)...);
        T &ref = *ptr;
        m_children.emplace(name, std::move(ptr));
        return ref;
    }

    Result<Node*> find(std::string_view path) override;

};

class DirHandle: public Dragonstash::Backend::Dir {
public:
    DirHandle() = delete;
    explicit DirHandle(Directory &node);
    ~DirHandle() override;

private:
    enum State {
        DOT,
        DOTDOT,
        ITERATION,
    };

    Directory *m_node;
    State m_state;
    Directory::Children::iterator m_iter;

    // Dir interface
public:
    Result<DirEntry> readdir() override;
    Result<void> fsyncdir() override;
    Result<void> closedir() override;
};

}

class InMemoryFilesystem: public Filesystem, public InMemory::Directory {
private:
    bool m_connected{true};

public:
    [[nodiscard]] inline bool connected() const {
        return m_connected;
    }

    void set_connected(bool connected);

    // Filesystem interface
public:
    [[nodiscard]] Result<std::unique_ptr<File> > open(std::string_view path, int accesstype, mode_t mode) override;
    [[nodiscard]] Result<std::unique_ptr<Dir> > opendir(std::string_view path) override;
    [[nodiscard]] Result<Stat> lstat(std::string_view path) override;
    [[nodiscard]] Result<std::string> readlink(std::string_view path) override;
};

}

#endif
