#include "cache/blocklist.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>

#include "cache/common.hpp"


namespace Dragonstash {


FileHandle::FileHandle():
    m_fd(-1)
{

}

FileHandle::FileHandle(int fd):
    m_fd(fd)
{

}

FileHandle::FileHandle(FileHandle &&src) noexcept:
    m_fd(src.release())
{

}

FileHandle &FileHandle::operator=(FileHandle &&src) noexcept
{
    reset(src.release());
    return *this;
}

FileHandle::~FileHandle()
{
    reset(-1);
}

void FileHandle::reset(int other_fd) noexcept
{
    if (*this) {
        ::close(m_fd);
    }
    m_fd = other_fd;
}


Blocklist::Blocklist(FileHandle fd):
    m_fd(std::move(fd)),
    m_mapping(nullptr)
{
    ensure_mapped();
}

Blocklist::Blocklist(const std::filesystem::path &path):
    Blocklist(open(path))
{

}

Blocklist::~Blocklist()
{
    ensure_unmapped();
    if (m_fd) {
        fsync(int(m_fd));
    }
}

FileHandle Blocklist::open(const std::filesystem::path &path)
{
    FileHandle result(::open(path.c_str(),
                             O_CREAT | O_RDWR | O_CLOEXEC,
                             S_IRUSR | S_IWUSR));
    if (!result) {
        throw std::runtime_error(std::string("failed to open blocklist: ") + std::strerror(errno));
    }

    struct stat buf{};
    int rc = ::fstat(int(result), &buf);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to read blocklist: ") + std::strerror(errno));
    }
    if ((buf.st_mode & S_IFMT) != S_IFREG) {
        throw std::runtime_error("not a regular file");
    }
    if (buf.st_size % internal_block_size != 0) {
        throw std::runtime_error("incompatible or corrupted blocklist file");
    }

    if (buf.st_size == 0) {
        // initialise
        rc = ::ftruncate(int(result), initial_block_count * internal_block_size);
        if (rc != 0) {
            throw std::runtime_error("failed to initialise blocklist file");
        }
        Superblock header{
            .magic = magic,
        };
        ssize_t written = pwrite(int(result), &header, sizeof(header), 0);
        if (written != sizeof(header)) {
            throw std::runtime_error("failed to write superblock");
        }
    } else {
        // check
        Superblock header{};
        ssize_t read = pread(int(result), &header, sizeof(header), 0);
        if (read != sizeof(header)) {
            throw std::runtime_error("failed to read superblock");
        }
        if (header.magic != magic) {
            throw std::runtime_error("invalid magic");
        }
    }

    return result;
}

void Blocklist::ensure_mapped() const
{
    if (m_mapping) {
        return;
    }
    struct stat buf{};
    int rc = ::fstat(int(m_fd), &buf);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to read blocklist: ") + std::strerror(errno));
    }

    assert((buf.st_mode & S_IFMT) == S_IFREG);
    m_mapping = reinterpret_cast<File*>(mmap(nullptr, buf.st_size,
                                             PROT_READ | PROT_WRITE,
                                             MAP_SHARED,
                                             int(m_fd), 0));
    if (!m_mapping) {
        throw std::runtime_error(std::string("failed to map blocklist: ") + std::strerror(errno));
    }
    m_mapped_size = buf.st_size;
}

void Blocklist::ensure_unmapped() const
{
    if (!m_mapping) {
        return;
    }
    int rc = munmap(m_mapping, m_mapped_size);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to unmap blocklist: ") + std::strerror(errno));
    }
    m_mapping = nullptr;
}

void Blocklist::grow()
{
    ensure_unmapped();
    struct stat buf{};
    int rc = ::fstat(int(m_fd), &buf);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to read blocklist: ") + std::strerror(errno));
    }
    const std::size_t new_size = buf.st_size + grow_size;
    assert((buf.st_mode & S_IFMT) == S_IFREG);
    rc = ::ftruncate(int(m_fd), new_size);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to grow blocklist: ") + std::strerror(errno));
    }
}

void Blocklist::require_space()
{
    if (capacity() == nentries()) {
        grow();
        ensure_mapped();
    }
    assert(capacity() > nentries());
}

Blocklist::File::iterator Blocklist::delete_entry(File::iterator iter)
{
    ensure_mapped();
    if (iter == m_mapping->end()) {
        return iter;
    }
    return delete_range(iter, iter + 1);
}

Blocklist::File::iterator Blocklist::delete_range(Blocklist::File::iterator begin,
                                                  Blocklist::File::iterator end)
{
    const auto index = begin - m_mapping->begin();
    const auto deleted = end - begin;
    const auto to_move = m_mapping->end() - end;

    /* std::cout << "delete: index=" << index
              << ", deleted=" << deleted
              << ", to_move=" << to_move
              << "; nentries=" << m_mapping->superblock.entries << std::endl; */

    assert(to_move + deleted <= m_mapping->superblock.entries);

    // update block bookkeeping
    for (auto iter = begin; iter != end; ++iter) {
        m_mapping->superblock.blocks_by_state[iter->state] -= iter->count;
    }

    memmove(&m_mapping->entries[index],
            &m_mapping->entries[index + deleted],
            to_move * sizeof(Entry));
    m_mapping->superblock.entries -= deleted;

    return &m_mapping->entries[index];
}

std::pair<bool, Blocklist::File::iterator> Blocklist::try_merge_with_previous(Blocklist::File::iterator iter)
{
    const auto iter_begin = m_mapping->begin();

    if (iter == iter_begin) {
        // std::cout << "merge rejected: at begin" << std::endl;
        // at start of range, no previous entry -> no join
        return std::make_pair(false, iter);
    }

    auto prev = iter - 1;
    if (prev->end() != iter->start) {
        // std::cout << "merge rejected: range not adjacent" << std::endl;
        // end of previous is different from start -> no join
        return std::make_pair(false, iter);
    }

    if (prev->state != iter->state) {
        // std::cout << "merge rejected: state mismatch" << std::endl;
        return std::make_pair(false, iter);
    }

    const std::uint64_t new_count = std::uint64_t(prev->count) + std::uint64_t(iter->count);
    if (new_count > std::numeric_limits<entry_block_count_type>::max()) {
        // std::cout << "merge rejected: resulting range too large" << std::endl;
        // new count would exceed limits -> no join
        return std::make_pair(false, iter);
    }

    prev->count = new_count;
    // we have to add the count to the bookkeeping because delete_entry will
    // remove it.
    m_mapping->superblock.blocks_by_state[prev->state] += iter->count;
    return std::make_pair(true, delete_entry(iter) - 1);
}

Blocklist::File::iterator Blocklist::insert_before(File::iterator dest,
        Blocklist::Entry &&entry)
{
    const auto insert_index = dest - m_mapping->begin();
    const auto to_move = m_mapping->end() - dest;

    require_space();  // invalidates all iterators!
    if (to_move) {
        memmove(&m_mapping->entries[insert_index + 1],
                &m_mapping->entries[insert_index],
                to_move * sizeof(Entry));
    };

    m_mapping->entries[insert_index] = entry;
    ++m_mapping->superblock.entries;

    return &m_mapping->entries[insert_index];
}

Blocklist::File::iterator Blocklist::split_entry(Blocklist::File::iterator at,
                                                 std::uint64_t split_point)
{
    assert(at->start <= split_point);
    assert(at->end() > split_point);

    const auto old_end = at->end();
    const auto old_count = at->count;
    Entry new_entry = *at;
    new_entry.start = split_point;
    new_entry.count = old_end - split_point;
    at->count = split_point - at->start;
    at = insert_before(at + 1, std::move(new_entry)) - 1;

    assert(at->end() == split_point);
    assert((at+1)->end() == old_end);
    assert(at->count + (at+1)->count == old_count);

    return at;
}

Blocklist::File::iterator Blocklist::search_entry(std::uint64_t block)
{
    ensure_mapped();
    auto result = std::upper_bound(
                m_mapping->begin(),
                m_mapping->end(),
                block,
                [](std::uint64_t block, const Entry &entry){
        return block < entry.end();
    });
    /* std::cout << "search: target=" << block
              << std::endl;
    dump(std::cout);
    std::cout << "-> " << escape_iterator(result) << std::endl; */
    return result;
}

Blocklist::File::const_iterator Blocklist::search_entry(uint64_t block) const
{
    return const_cast<Blocklist*>(this)->search_entry(block);
}



Blocklist::File::difference_type Blocklist::escape_iterator(Blocklist::File::iterator iter) const
{
    return iter - m_mapping->begin();
}

std::pair<Blocklist::File::iterator, Blocklist::File::iterator> Blocklist::find_overlapping_entries(uint64_t start, Blocklist::entry_block_count_type count)
{
    ensure_mapped();
    if (m_mapping->begin() == m_mapping->end()) {
        // empty range -> early out
        // this saves some checks down below
        return std::make_pair(m_mapping->end(), m_mapping->end());
    }

    const std::uint64_t end = start + count;
    File::iterator start_overlap = search_entry(start);
    if (start_overlap == m_mapping->end()) {
        return std::make_pair(m_mapping->end() - 1, m_mapping->end());
    }
    if (!start_overlap->contains(start)) {
        // start_overlap points at the element *behind* the start, because there
        // is no element overlapping it.
        if (start_overlap == m_mapping->begin()) {
            // no possible value for start_overlap, set it to the out-of-range
            // iterator
            start_overlap = m_mapping->end();
        } else {
            // make start_overlap point at the entry *before* `start`.
            --start_overlap;
        }
    }

    File::iterator end_overlap = search_entry(end);
    return std::make_pair(start_overlap, end_overlap);
}

void Blocklist::mark_internal(const uint64_t start,
                              const Blocklist::entry_block_count_type count,
                              const Blocklist::State state)
{
    /* next strategy:
     * implement function returning two iterators: `before` and `after`.
     * - `before` >= `after`
     * - `before` == `after` <=> new range is fully contained within other entry
     * - `after` - `before` > 1 => new range fully covers at least one existing
     *   entry
     * - `before` points at the entry overlapping with `start` or the last
     *   entry whose end is before or at `start`
     * - `after` points at the entry overlapping with `end` or at the first
     *   entry whose start is after `end`
     *
     * then we can handle intersection of start with `before` and end with
     * `after` in clean code, delete all entries in-between, insert the new
     * entry and attempt merges with the `before` and `after` entry.
     *
     * bonus if we don’t even have to treat `before` = `after` specially.
     *
     * general algorithm:
     * - handle overlap with `after` by splitting at `start`
     * - handle overlap with `before` by splitting at `end`
     * - delete everything between `after` and `before`, including split items
     * - insert new item before `before`
     * - try to merge `before` into new item and new item into `after`
     */

    // breaks indentation of QtCreator :(
    // auto [start_overlap, end_overlap] = find_overlapping_entries(start, count);

    Entry new_entry{
        .start = start,
        .count = count,
        .state = std::uint8_t(state)
    };
    const std::uint64_t end = start + count;
    File::iterator start_overlap;
    File::iterator end_overlap;
    std::tie(start_overlap, end_overlap) = find_overlapping_entries(start, count);

    // 1. neither start nor end overlaps (-> delete, insert)
    // 2. start overlaps, end does not (-> truncate start, delete, insert)
    // 3. both overlap but are not equal (-> truncate start, truncate end, delete, insert)
    // 4. end overlaps, start does not (-> truncate end, delete, insert)
    // 5. both overlap and are equal (-> split start/end, insert)

    // case 5: an entry fully contains the new one
    if (start_overlap != m_mapping->end() && start_overlap == end_overlap) {
        // this requires creation of two new entries. we split the start overlap
        // and then continue as normal, pretending that the two split parts
        // were the two entries we found earlier. the start will at this point
        // not overlap anymore and only the end needs to be taken care of.
        start_overlap = split_entry(start_overlap, start);  // invalidates all iterators!
        end_overlap = start_overlap + 1;
    }

    const bool start_contains = start_overlap != m_mapping->end() && start_overlap->contains(start);
    const bool end_contains = end_overlap != m_mapping->end() && end_overlap->contains(end - 1);

    // case 2: start overlaps, end does not
    if (start_contains) {
        // check for exact match:
        if (start_overlap->start == start && start_overlap->count <= count) {
            if (state == ABSENT) {
                delete_range(start_overlap, end_overlap);
                return;
            }

            m_mapping->superblock.blocks_by_state[start_overlap->state] -= start_overlap->count;
            *start_overlap = new_entry;
            m_mapping->superblock.blocks_by_state[state] += count;
            // we still have to merge and delete manually here
            auto item = delete_range(start_overlap+1, end_overlap) - 1;
            auto next = item + 1;
            if (next != m_mapping->end()) {
                bool success;
                File::iterator merged;
                std::tie(success, merged) = try_merge_with_previous(next);
                if (success) {
                    item = merged;
                }
            }
            try_merge_with_previous(item);
            return;
        }
        const auto old_count = start_overlap->count;
        start_overlap->count = start - start_overlap->start;
        assert(start_overlap->end() == start);
        assert(old_count > start_overlap->count);
        assert(start_overlap->count > 0);
        m_mapping->superblock.blocks_by_state[start_overlap->state] -= (old_count - start_overlap->count);
    }

    // case 4: end overlaps, start does not
    if (end_contains) {
        const auto old_end = end_overlap->end();
        const auto old_count = end_overlap->count;
        end_overlap->start = end;
        end_overlap->count = old_end - end;
        assert(!end_overlap->contains(end-1));
        assert(old_count > end_overlap->count);
        m_mapping->superblock.blocks_by_state[end_overlap->state] -= (old_count - end_overlap->count);
    }

    // case 1: neither start nor end overlap
    File::iterator delete_begin = start_overlap;
    if (delete_begin == m_mapping->end()) {
        delete_begin = m_mapping->begin();
    } else {
        ++delete_begin;
    }
    const File::iterator delete_end = end_overlap;

    /* std::cout << "start overlap: " << escape_iterator(start_overlap) << std::endl;
    std::cout << "end overlap: " << escape_iterator(end_overlap) << std::endl;
    std::cout << "delete begin: " << escape_iterator(delete_begin) << std::endl;
    std::cout << "delete end: " << escape_iterator(delete_end) << std::endl; */

    auto insert_at = delete_range(delete_begin, delete_end);  // invalidates all iterators!

    if (state == ABSENT) {
        return;
    }

    auto inserted = insert_before(insert_at, std::move(new_entry));  // invalidates all iterators!
    m_mapping->superblock.blocks_by_state[inserted->state] += inserted->count;

    if (inserted != m_mapping->end()) {
        auto next = inserted + 1;
        if (inserted != m_mapping->end()) {
            bool success;
            File::iterator merged;
            std::tie(success, merged) = try_merge_with_previous(next);   // invalidates all iterators!
            if (success) {
                inserted = merged;
            }
        }
    }

    if (inserted != m_mapping->begin()) {
        try_merge_with_previous(inserted);
    }
}

void Blocklist::mark(uint64_t start, uint64_t count, Blocklist::State state)
{
    ensure_mapped();
    static constexpr auto limit = std::numeric_limits<entry_block_count_type>::max();
    while (count > limit) {
        mark_internal(start, limit, state);
        start += limit;
        count -= limit;
    }
    if (count > 0) {
        mark_internal(start, count, state);
    }
}

Blocklist::State Blocklist::state(uint64_t block) const
{
    ensure_mapped();
    for (std::size_t i = 0; i < m_mapping->superblock.entries; ++i) {
        const auto &entry = m_mapping->entries[i];
        if (entry.start <= block) {
            std::uint64_t end = entry.start + entry.count;
            if (end > block) {
                return State(entry.state);
            }
        }
    }
    return ABSENT;
}

uint64_t Blocklist::blocks(Blocklist::State state) const
{
    if (state == ABSENT) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    ensure_mapped();
    return m_mapping->superblock.blocks_by_state[state];
}

uint64_t Blocklist::present_blocks() const
{
    auto superblock_guard = temporary_superblock();
    const Superblock &superblock = superblock_guard.superblock();

    std::uint64_t total_blocks = 0;
    for (auto nblocks: superblock.blocks_by_state) {
        total_blocks += nblocks;
    }
    return total_blocks;
}

uint64_t Blocklist::size() const
{
    return temporary_superblock().superblock().size;
}

uint64_t Blocklist::nentries() const
{
    return temporary_superblock().superblock().entries;
}

uint64_t Blocklist::capacity() const
{
    ensure_mapped();
    return (m_mapped_size - sizeof(File)) / sizeof(Entry);
}

std::size_t Blocklist::truncate_access(off_t start, std::size_t size) const
{
    if (start < 0) {
        return 0;
    }
    const std::uint64_t start_block = start / CACHE_PAGE_SIZE;
    const std::uint64_t requested_end_block = (start + size + CACHE_PAGE_SIZE - 1) / CACHE_PAGE_SIZE;
    auto start_block_iter = search_entry(start_block);
    if (start_block_iter == m_mapping->end()) {
        return 0;
    }
    if (!start_block_iter->contains(start_block)) {
        return 0;
    }
    std::uint64_t end_of_available_range = start_block_iter->end();
    while (start_block_iter != m_mapping->end() &&
           end_of_available_range < requested_end_block)
    {
        ++start_block_iter;
        if (start_block_iter->start != end_of_available_range) {
            break;
        }
        end_of_available_range = start_block_iter->end();
    }
    const std::uint64_t end_block = std::min(requested_end_block + 1,
                                             end_of_available_range);
    const std::size_t max_length = (end_block - start_block) * CACHE_PAGE_SIZE;
    return std::min(size, max_length);
}

void Blocklist::fsck() const
{
    ensure_mapped();
    const auto iter_begin = m_mapping->begin();
    const auto iter_end = m_mapping->end();
    std::uint64_t prev_end = 0;
    std::uint64_t prev_start = 0;
    std::array<std::uint64_t, 4> blocks_by_state{};
    for (auto iter = iter_begin;
         iter != iter_end;
         ++iter)
    {
        const auto index = iter - iter_begin;
        if (iter->start < prev_end) {
            throw std::runtime_error(
                        std::string("inconsistency detected: at ") +
                        std::to_string(index) + ": entry start is at " +
                        std::to_string(iter->start) + ", but previous end is "
                        "at " + std::to_string(prev_end)
                        );
        }
        if (iter->start < prev_start) {
            throw std::runtime_error(
                        std::string("inconsistency detected: at ") +
                        std::to_string(index) + ": entry start is at " +
                        std::to_string(iter->start) + ", but previous start is "
                        "at " + std::to_string(prev_start)
                        );
        }
        if (iter->count == 0) {
            throw std::runtime_error(
                        std::string("inconsistency detected: at ") +
                        std::to_string(index) + ": entry count is zero"
                        );
        }
        blocks_by_state[iter->state] += iter->count;
        prev_start = iter->start;
        prev_end = iter->end();
    }

    for (std::size_t i = 0; i < blocks_by_state.size(); ++i) {
        if (blocks_by_state[i] != m_mapping->superblock.blocks_by_state[i]) {
            throw std::runtime_error("inconsistency detected: block bookkeeping is off for state "+std::to_string(i)+": expected "+std::to_string(blocks_by_state[i])+" but superblock contains "+std::to_string(m_mapping->superblock.blocks_by_state[i]));
        }
    }

    shrink();
}

void Blocklist::shrink() const
{
    auto superblock_guard = temporary_superblock();
    const Superblock &superblock = superblock_guard.superblock();

    const std::size_t curr_grow_steps = m_mapped_size / grow_size;
    if (curr_grow_steps <= 1) {
        return;
    }

    const std::size_t required_grow_steps = (
                /* space needed for superblock */
                sizeof(Superblock) +
                /* space needed for current entries */
                superblock.entries * sizeof(Entry) +
                /* round up to full grow size */
                grow_size - 1) / grow_size;

    if (required_grow_steps == curr_grow_steps) {
        return;
    }

    const std::size_t new_file_size = required_grow_steps * grow_size;
    const std::size_t new_capacity = (new_file_size - sizeof(Superblock)) / sizeof(Entry);
    assert(new_file_size < m_mapped_size);
    // double-assert that we don’t truncate away entries we have...
    assert(new_capacity >= superblock.entries);
    ensure_unmapped();
    int rc = ftruncate(int(m_fd), new_file_size);
    if (rc != 0) {
        throw std::runtime_error(std::string("failed to shrink blocklist: ") +
                                 std::strerror(errno));
    }
}

std::ostream &Blocklist::dump(std::ostream &out) const
{
    ensure_mapped();
    out << "Blocklist!{" << std::endl;
    out << "  blocks_by_state = {" << std::endl;
    for (std::size_t i = 0; i < m_mapping->superblock.blocks_by_state.size();
         ++i)
    {
        out << "    [" << i << "] = "
            << m_mapping->superblock.blocks_by_state[i] << std::endl;
    }
    out << "  };" << std::endl;
    out << "  {" << std::endl;
    for (auto iter = m_mapping->begin();
         iter != m_mapping->end();
         ++iter)
    {
        const auto &entry = *iter;
        out << "    Entry{.start = " << entry.start << ", "
            << ".count = " << entry.count << " (end: " << entry.end() << "), "
            << ".state = " << int(entry.state) << "}," << std::endl;
    }
    out << "  }" << std::endl;
    return out << "}" << std::endl;
}

Blocklist::SuperblockGuard::SuperblockGuard(const Blocklist::Superblock &superblock):
    m_superblock(&superblock)
{

}

Blocklist::SuperblockGuard::SuperblockGuard(int fd):
    m_superblock(read_superblock(fd))
{

}

Blocklist::Superblock Blocklist::SuperblockGuard::read_superblock(int fd)
{
    Superblock result;
    ssize_t read = ::pread(fd, &result, sizeof(Superblock), 0);
    if (read != sizeof(Superblock)) {
        throw std::runtime_error(
                    std::string("failed to read superblock (partial read): ") +
                    std::strerror(errno));
    }

    return result;
}

}
