/**********************************************************************
File name: error.hpp
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
#ifndef DRAGONSTASH_ERROR_H
#define DRAGONSTASH_ERROR_H

#include <optional>

namespace Dragonstash {

enum failed_t {
    FAILED = 0,
};

struct ErrorResultHelper {
    int error;
};

template <typename T>
struct Result {
public:
    template<typename U, typename _ = typename std::enable_if<std::is_convertible<U, T>::value>::type>
    Result(U &&value):
        m_value(std::forward<U>(value)),
        m_errno(0)
    {

    }

    template<typename U>
    Result(const Result<U> &src):
        m_value(bool(src) ? *src : T()),
        m_errno(src.error())
    {

    }

    template<typename U>
    Result(Result<U> &&src):
        m_value(bool(src) ? std::move(*src) : T()),
        m_errno(src.error())
    {

    }

    Result(failed_t, int err):
        m_value(),
        m_errno(err)
    {

    }

    Result(ErrorResultHelper &&helper):
        Result(FAILED, helper.error)
    {

    }

    Result(const Result &ref) = default;
    Result(Result &&src) noexcept = default;
    Result &operator=(const Result &ref) = default;
    Result &operator=(Result &&src) noexcept = default;

    ~Result() = default;

private:
    std::optional<T> m_value;
    int m_errno;

public:
    inline explicit operator bool() const {
        return m_value.has_value();
    }

    [[nodiscard]] inline int error() const {
        return m_errno;
    }

    [[nodiscard]] inline T &get() const {
        return &m_value.value();
    }

    inline T &operator*() {
        return m_value.value();
    }

    inline const T &operator*() const {
        return m_value.value();
    }

    inline T *operator->() {
        return &m_value.value();
    }

    inline const T *operator->() const {
        return &m_value.value();
    }

};

template<>
struct Result<void> {
public:
    Result():
        m_ok(true),
        m_errno(0)
    {

    }

    Result(const Result &src) = default;
    Result(Result &&src) = default;
    Result &operator=(const Result &src) = default;
    Result &operator=(Result &&src) = default;

    Result(ErrorResultHelper &&helper):
        m_ok(false),
        m_errno(helper.error)
    {

    }

    Result(failed_t, int err):
        m_ok(false),
        m_errno(err)
    {

    }

private:
    bool m_ok;
    int m_errno;

public:
    inline explicit operator bool() const {
        return m_ok;
    }

    [[nodiscard]] inline int error() const {
        return m_errno;
    }

};


template<typename T>
[[nodiscard]] inline Result<typename std::remove_reference<T>::type> make_result(T &&value)
{
    return Result<typename std::remove_reference<T>::type>(std::forward<T>(value));
}

[[nodiscard]] inline ErrorResultHelper make_result(failed_t, int err)
{
    return ErrorResultHelper{err};
}

template<typename T>
[[nodiscard]] inline ErrorResultHelper copy_error(const Result<T> &result)
{
    return ErrorResultHelper{result.error()};
}

[[nodiscard]] inline Result<void> make_result()
{
    return Result<void>();
}


}

#endif
