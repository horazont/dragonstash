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
    template<typename U>
    Result(U &&value):
        m_value(std::forward<U>(value)),
        m_errno(0)
    {

    }

    Result(ErrorResultHelper &&helper):
        Result(FAILED, helper.error)
    {

    }

    Result(failed_t, int err):
        m_value(),
        m_errno(err)
    {

    }

    Result(const Result &ref) = delete;
    Result(Result &&src) = default;
    Result &operator=(const Result &ref) = delete;
    Result &operator=(Result &&src) = default;

private:
    std::optional<T> m_value;
    int m_errno;

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

    inline const T &operator*() const {
        return m_value.value();
    }

    inline T &operator*() {
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


template<typename T>
inline Result<T> make_result(T &&value)
{
    return Result<T>(std::forward<T>(value));
}

inline ErrorResultHelper make_result(failed_t, int err)
{
    return ErrorResultHelper{err};
}

inline Result<void> make_result()
{
    return Result<void>();
}


}

#endif
