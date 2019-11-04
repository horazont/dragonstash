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
