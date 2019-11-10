/**********************************************************************
File name: tempdir.cpp
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
#include "tempdir.hpp"

#include <random>
#include <cerrno>
#include <cstring>

#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>


std::string random_name()
{
    std::random_device dev;
    std::mt19937_64 engine(dev());
    std::uniform_int_distribution<std::uint64_t> distribution;
    const std::uint64_t n1 = distribution(engine);
    std::stringstream ss;
    ss << std::hex << std::setw(sizeof(n1) * 2) << std::setfill('0') << std::right
       << n1;
    return ss.str();
}


std::filesystem::path custom_mkdtemp()
{
    const char *tmpdir = getenv("DRAGONSTASH_TEST_TMP_DIR");
    if (!tmpdir) {
        tmpdir = getenv("TMPDIR");
    }
    if (!tmpdir) {
        tmpdir = "/tmp";
    }
    std::filesystem::path basepath(tmpdir);
    std::filesystem::path final = basepath;
    int rc = 0;
    do {
        final /= "dragonstash-test-" + random_name();
        rc = mkdir(final.c_str(), 0700);
    } while (rc != 0 && errno == EEXIST);
    if (rc !=0) {
        throw std::runtime_error(
                    std::string("failed to create temporary directory: ") +
                    std::strerror(errno));
    }
    return final;
}


TemporaryDirectory::TemporaryDirectory():
    m_path(custom_mkdtemp())
{

}

TemporaryDirectory::~TemporaryDirectory()
{
    std::filesystem::remove_all(m_path);
}
