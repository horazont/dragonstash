/**********************************************************************
File name: debug_mutex.hpp
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
