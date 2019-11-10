/**********************************************************************
File name: blocklist.hpp
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
#ifndef DRAGONSTASH_CACHE_BLOCKLIST_H
#define DRAGONSTASH_CACHE_BLOCKLIST_H

#include <filesystem>
#include <variant>

namespace Dragonstash {

class FileHandle
{
public:
    FileHandle();
    explicit FileHandle(int fd);
    FileHandle(const FileHandle &src) = delete;
    FileHandle(FileHandle &&src) noexcept;
    FileHandle &operator=(const FileHandle &src) = delete;
    FileHandle &operator=(FileHandle &&src) noexcept;
    ~FileHandle();

private:
    int m_fd;

public:
    explicit inline operator int() const {
        return m_fd;
    }

    [[nodiscard]] inline int release() noexcept {
        int result = m_fd;
        m_fd = -1;
        return result;
    }

    explicit inline operator bool() const {
        return m_fd >= 0;
    }

    void reset(int other_fd) noexcept;
};

/*
 * Blocklist layout:
 *
 * - Superblock (512 B):
 *   - std::uint64_t size
 *   - std::uint8_t reserved[512-sizeof(std::uint64_t)]
 * - Listblocks (512 B each):
 *   - Array of entries (16 B each)
 *     - std::uint64_t start
 *     - std::uint16_t count
 *     - std::uint16_t unused
 *     - std::uint32_t unused
 */

class Blocklist
{
public:
    enum State {
        ABSENT = -1,
        READAHEAD = 0,
        READ = 1,
        PINNED = 2,
        WRITTEN = 3,
    };

private:
    static constexpr std::uint32_t magic = 0x4c427344;  /* b'DsBL' */
    static constexpr std::size_t internal_block_size = 512;
    static constexpr std::size_t grow_size = 4096;
    static constexpr std::size_t initial_block_count = grow_size / internal_block_size;
    static_assert(grow_size % internal_block_size == 0);
    static_assert(grow_size >= internal_block_size);

    using entry_block_count_type = std::uint16_t;

    struct Superblock
    {
        std::uint32_t magic;
        std::uint8_t version;
        std::array<std::uint8_t, 3> reserved1;
        std::uint64_t size;
        std::uint64_t entries;
        std::array<std::uint64_t, 4> blocks_by_state;
        std::array<std::uint8_t, 512-56> reserved_fin;
    };

    static_assert(sizeof(Superblock) == internal_block_size);
    static_assert(std::is_pod<Superblock>::value);

    struct Entry
    {
        std::uint64_t start;
        entry_block_count_type count;
        std::uint8_t state;
        std::uint8_t reserved1;
        std::uint32_t reserved2;

        [[nodiscard]] inline std::uint64_t end() const {
            return start + count;
        }

        [[nodiscard]] inline bool contains(std::size_t block) const {
            return start <= block && block < end();
        }
    };

    static_assert(sizeof(Entry) == 16);
    static_assert(alignof(Entry) <= 16);
    static_assert(std::is_pod<Entry>::value);

    struct File
    {
        using value_type = Entry;
        using reference = Entry&;
        using const_reference = const Entry&;
        using iterator = Entry*;
        using const_iterator = const Entry*;
        using difference_type = std::iterator_traits<iterator>::difference_type;
        using size_type = std::size_t;

        Superblock superblock;
        Entry entries[];

        [[nodiscard]] inline size_type size() const {
            return superblock.entries;
        }

        [[nodiscard]] inline iterator begin() {
            return &entries[0];
        }

        [[nodiscard]] inline const_iterator cbegin() const {
            return &entries[0];
        }

        [[nodiscard]] inline const_iterator begin() const {
            return cbegin();
        }

        [[nodiscard]] inline iterator end() {
            return &entries[size()];
        }

        [[nodiscard]] inline const_iterator cend() const {
            return &entries[size()];
        }

        [[nodiscard]] inline const_iterator end() const {
            return cend();
        }
    };

    static_assert(sizeof(File) == sizeof(Superblock));
    static_assert(alignof(File) <= 16);
    static_assert(std::is_pod<File>::value);

public:
    Blocklist() = delete;
    explicit Blocklist(FileHandle fd);
    explicit Blocklist(const std::filesystem::path &path);
    ~Blocklist();

private:
    FileHandle m_fd;
    mutable File *m_mapping;
    mutable std::size_t m_mapped_size;

    struct SuperblockGuard
    {
    public:
        explicit SuperblockGuard(const Superblock &superblock);
        explicit SuperblockGuard(int fd);

    private:
        static Superblock read_superblock(int fd);

    private:
        std::variant<const Superblock*, Superblock> m_superblock;

    public:
        [[nodiscard]] inline const Superblock &superblock() const {
            const Superblock *const *ref = std::get_if<const Superblock*>(&m_superblock);
            if (ref) {
                return **ref;
            }
            return std::get<Superblock>(m_superblock);
        }

    };

private:
    static FileHandle open(const std::filesystem::path &path);
    void ensure_mapped() const;
    void ensure_unmapped() const;

    /**
     * @brief Grow the mapping by grow_size bytes.
     *
     * This invalidates all iterators, because there is no guarantee that the
     * mapping will live at the same address at it was before.
     */
    void grow();

    /**
     * @brief Ensure that at least one more entry can be stored.
     *
     * If necessary, this calls grow().
     */
    void require_space();

    /**
     * @brief Return a handle to current Superblock data.
     *
     * If the file is currently mapped, the returned object will reference
     * the currently mapped superblock. If the file is currently unmapped,
     * the superblock will be read from the file and into memory.
     *
     * The superblock is valid for the lifetime of the returned object or until
     * ensure_unmapped() is called.
     *
     * Note that there is no guarantee that updates to the superblock through
     * other methods will become visible to the returned object.
     *
     * This should only be used in methods which:
     * - only read a single value from the superblock once and/or
     * - which will unmap the file "soon" after accessing the superblock, thus
     *   using this may possible save a map/unmap roundtrip.
     */
    [[nodiscard]] inline SuperblockGuard temporary_superblock() const {
        if (!m_mapping) {
            return SuperblockGuard(int(m_fd));
        }
        return SuperblockGuard(m_mapping->superblock);
    }

    File::iterator delete_entry(File::iterator iter);

    /**
     * Delete a range of entries.
     *
     * @param begin Iterator pointing at the first entry to delete.
     * @param end Iterator pointing behind the last entry to delete.
     *
     * @return Iterator pointing at the entry behind the last deleted one.
     */
    File::iterator delete_range(File::iterator begin, File::iterator end);

    /**
     * Try to merge an entry with the previous entry.
     *
     * @param iter Iterator pointing at the entry to merge into the previous
     *   one.
     *
     * If a merge is possible, returns an iterator to the merged entry.
     * If a merge is not possible, returns the iterator unchanged.
     *
     * @return Iterator pointing to the previous entry or the merged entry.
     */
    std::pair<bool, File::iterator> try_merge_with_previous(File::iterator iter);

    /**
     * Insert an entry before another.
     *
     * @param dest The entry before which to insert.
     * @param entry New entry to insert.
     *
     * Invalidates all iterators.
     *
     * @return An iterator pointing at the newly inserted entry.
     */
    File::iterator insert_before(File::iterator dest, Entry &&entry);

    /**
     * Split an entry in two parts
     *
     * @param at Iterator pointing at the entry to split.
     * @param split_point The (absolute) block number at which to split the
     *   entry.
     *
     * Behaviour becomes undefined if !at->contains(split_point).
     *
     * This method invalidates all iterators.
     *
     * @return Iterator pointing at the first of the two halves after splitting.
     */
    File::iterator split_entry(File::iterator at, std::uint64_t split_point);

    /**
     * Return the "closest" entry containing block.
     *
     * @param block The number of the block to search an entry for.
     *
     * If no entry exists, this function returns the first entry describing
     * a block following the block pointed to by `block`.
     *
     * @return Iterator pointing at the entry.
     */
    File::iterator search_entry(std::uint64_t block);
    File::const_iterator search_entry(std::uint64_t block) const;

    File::difference_type escape_iterator(File::iterator iter) const;

    std::pair<File::iterator, File::iterator> find_overlapping_entries(
            std::uint64_t start,
            entry_block_count_type count);
    void mark_internal(std::uint64_t start,
                       entry_block_count_type count,
                       State state);

public:
    /**
     * @brief Set the state of a range of blocks.
     *
     * @param start First block to mark.
     * @param count Number of blocks to mark, starting at the block referred to
     *   by `start`.
     * @param state The new state of the blocks.
     *
     * Note: All counts beyond an internal limit (currently 65535) will be
     * inefficient (but more efficient than splitting up the marks yourself).
     */
    void mark(std::uint64_t start,
              std::uint64_t count,
              State state);

    /**
     * @brief Query the state of a single block.
     *
     * @param block The number of the block (starting at zero) to query.
     * @return The state of the block. If the block is not marked as present,
     * this returns the ABSENT state.
     *
     * Note: this is the *block number*, not a byte address.
     */
    [[nodiscard]] State state(std::uint64_t block) const;

    /**
     * @brief Return the number of blocks by state.
     *
     * @param state Select the state of blocks to count.
     * @return Number of blocks which are in the given `state`.
     *
     * Note that this is a O(1) operation since additional bookkeeping is in
     * place to count the number of blocks per state while states are modified
     * to avoid O(n) block counting.
     */
    [[nodiscard]] std::uint64_t blocks(State state) const;

    /**
     * @brief Return the number of blocks marked as some state other than
     * ABSENT.
     *
     * Effectively, this is a sum over blocks() for all valid states.
     */
    [[nodiscard]] std::uint64_t present_blocks() const;
    [[nodiscard]] std::uint64_t size() const;

    /**
     * @brief Return the current number of entries.
     *
     * This is not equal to the number of present blocks. See present_blocks().
     */
    [[nodiscard]] std::uint64_t nentries() const;

    /**
     * @brief Return the internal list capacity.
     *
     * Note that growth of the Blocklist happens transparently.
     *
     * @return Number of entries which can currently be held by the Blocklist
     *   without resizing.
     */
    [[nodiscard]] std::uint64_t capacity() const;

    /**
     * @brief Truncate an attempted access to the largest safe range.
     *
     * @param start The first byte of the attempted access.
     * @param size The number of bytes which are to be read by the access.
     * @return The number of bytes which are safe to read.
     *
     * This checks that all blocks are available to be read. If this is the
     * case, @arg size is returned.
     *
     * If a block is missing within the range, the maximum number of bytes
     * which can safely be read starting at @arg start is returned.
     *
     * If @arg start points to an absent block, by extension, zero is returned.
     */
    [[nodiscard]] std::size_t truncate_access(off_t start, std::size_t size) const;

    /**
     * @brief Check internal consistency and throw exceptions.
     *
     * Check the consistency of various redundant data in the internal data
     * structures as well as invariants.
     *
     * If any invariant is violated or an inconsistency is found, this raises
     * a std::runtime_error.
     *
     * In the future, we may fix inconsistencies using the most reliable data
     * and emit only a warning. Broken invariants will always raise an exception
     * and are for the caller to handle -- most likely by erasing the blocklist
     * and the accomanying data and starting from scratch.
     *
     * After passing all consistency checks, this calls shrink().
     */
    void fsck() const;

    /**
     * @brief Shrink the blocklist.
     *
     * This reduces the memory and disk space footprint of the blocklist if
     * it has grown in the past.
     *
     * Note that this will, in the general case, not reduce capacity() to
     * nentries(), since for efficiency reasons, the list operates on internal
     * blocks of entries. This means that capacity for entries is added/removed
     * approximately in steps of 256.
     *
     * Note that shrinking may unmap the file (without remapping it), so it
     * is an expensive-ish operation and implies a sync(), so it is not
     * performed automatically.
     */
    void shrink() const;

    /**
     * @brief Dump a human-readable text representation of the Blocklist.
     *
     * @param out Stream to dump the representation to.
     *
     * The representation is for debugging purposes only; it is not possible
     * to reconstruct a full Blocklist from it.
     */
    std::ostream &dump(std::ostream &out) const;
};

}

#endif
