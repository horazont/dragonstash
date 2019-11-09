#ifndef DRAGONSTASH_DEBUG_MUTEX_H
#define DRAGONSTASH_DEBUG_MUTEX_H

#include <mutex>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <cerrno>

namespace Dragonstash {

class debug_mutex
{
public:
    static constexpr bool is_safe =
        #ifndef NDEBUG
            true
        #else
            false
        #endif
            ;

private:
    std::mutex m_mutex;
#ifndef NDEBUG
    std::atomic<std::thread::id> m_current_owner;
#endif

public:
    inline void lock() {
#ifndef NDEBUG
        const auto my_id = std::this_thread::get_id();
        if (m_current_owner.load(std::memory_order_relaxed) == my_id) {
            throw std::system_error(std::error_code(EDEADLK, std::system_category()));
        }
#endif
        m_mutex.lock();
#ifndef NDEBUG
        m_current_owner.store(my_id, std::memory_order_relaxed);
#endif
    }

    inline void unlock() {
        m_current_owner.store(std::thread::id(), std::memory_order_relaxed);
        m_mutex.unlock();
    }

    inline bool try_lock() {
#ifndef NDEBUG
        const auto my_id = std::this_thread::get_id();
        if (m_current_owner.load(std::memory_order_relaxed) == my_id) {
            throw std::system_error(std::error_code(EDEADLK, std::system_category()));
        }
#endif
        const bool result = m_mutex.try_lock();
        if (result) {
#ifndef NDEBUG
            m_current_owner.store(my_id, std::memory_order_relaxed);
#endif
        }
        return result;
    }
};

}

#endif
