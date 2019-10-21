#ifndef DRAGONSTASH_ERROR_H
#define DRAGONSTASH_ERROR_H

#include <optional>

namespace Dragonstash {

enum failed_t {
    FAILED = 0,
};

template <typename T>
struct Result {
public:
    template<typename U>
    Result(U &&value):
        m_value(std::forward<U>(value)),
        m_errno(0)
    {

    }

    Result(failed_t, int err):
        m_value(),
        m_errno(err)
    {

    }

private:
    const std::optional<T> m_value;
    const int m_errno;

public:
    inline operator bool() const {
        return m_value.has_value();
    }

    inline int error() const {
        return m_errno;
    }

    inline T &get() const {
        return &m_value.value();
    }

    inline T &operator*() const {
        return m_value.value();
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

    Result(failed_t, int err):
        m_ok(false),
        m_errno(err)
    {

    }

private:
    const bool m_ok;
    const int m_errno;

public:
    inline operator bool() const {
        return m_ok;
    }

    inline int error() const {
        return m_errno;
    }

};


}

#endif
