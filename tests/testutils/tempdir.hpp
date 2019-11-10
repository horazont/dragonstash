/**********************************************************************
File name: tempdir.hpp
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
#ifndef DRAGONSTASH_TESTS_UTILS_TEMPDIR_H
#define DRAGONSTASH_TESTS_UTILS_TEMPDIR_H

#include <filesystem>
#include <string>

std::string random_name();
std::filesystem::path custom_mkdtemp();


class TemporaryDirectory {
public:
    TemporaryDirectory();
    TemporaryDirectory(const TemporaryDirectory &src) = delete;
    TemporaryDirectory(TemporaryDirectory &&src) = delete;
    TemporaryDirectory &operator=(const TemporaryDirectory &src) = delete;
    TemporaryDirectory &operator=(TemporaryDirectory &&src) = delete;
    ~TemporaryDirectory();

private:
    const std::filesystem::path m_path;

public:
    [[nodiscard]] inline const std::filesystem::path &path() const {
        return m_path;
    }

};


#endif
