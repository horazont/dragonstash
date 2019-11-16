/**********************************************************************
File name: common.hpp
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
#ifndef DRAGONSTASH_CACHE_COMMON_H
#define DRAGONSTASH_CACHE_COMMON_H

#include <cstdint>
#include <variant>

namespace Dragonstash {

static constexpr std::size_t CACHE_PAGE_SIZE = 4096;
static constexpr std::size_t CACHE_INODE_SIZE = CACHE_PAGE_SIZE / 16;

template <typename T>
class copyfree_wrap
{
public:
    copyfree_wrap() noexcept = default;

    explicit copyfree_wrap(const T &obj) noexcept:
        m_value(&obj)
    {

    }

    explicit copyfree_wrap(T &&obj) noexcept:
        m_value(std::move(obj))
    {

    }

    copyfree_wrap(const copyfree_wrap &src) = default;
    copyfree_wrap(copyfree_wrap &&src) noexcept = default;
    copyfree_wrap &operator=(const copyfree_wrap &src) = default;
    copyfree_wrap &operator=(copyfree_wrap &&src) noexcept = default;
    ~copyfree_wrap() = default;

private:
    using pointer_type = const T*;
    using value_type = T;

    std::variant<pointer_type, value_type> m_value;
    static_assert(sizeof(decltype(m_value)) >= sizeof(value_type));

public:
    [[nodiscard]] inline const T &value() const {
        if (std::holds_alternative<pointer_type>(m_value)) {
            return *std::get<pointer_type>(m_value);
        }
        return std::get<value_type>(m_value);
    }

    explicit operator bool() const {
        return !std::holds_alternative<pointer_type>(m_value) || std::get<pointer_type>(m_value);
    }

    const T &operator*() const {
        return value();
    }

    const T *operator->() const {
        return &value();
    }

    [[nodiscard]] T extract() const {
        if (std::holds_alternative<pointer_type>(m_value)) {
            return T(**this);
        }
        return std::get<value_type>(m_value);
    }
};

}

#endif
