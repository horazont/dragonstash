/**********************************************************************
File name: main.cpp
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
#include "dragonstash/fuse/request.hpp"
#include "dragonstash/fuse/buffer.hpp"
#include "dragonstash/fuse/interface.hpp"
#include "dragonstash/fs.hpp"

#include <iostream>
#include <cstring>

#include "dragonstash/backend/local.hpp"
#include "dragonstash/backend/in_memory.hpp"

#include <CLI/CLI.hpp>

class MountCommand
{
public:
    explicit MountCommand(CLI::App &app):
        m_cmd(*app.add_subcommand("mount", "Mount a dragonstash cache"))
    {

        auto &backend_group = *m_cmd.add_option_group("Backend");
        backend_group.require_option(1, 1);
        backend_group.add_flag("-N,--disconnected", "Mount without backend");
        backend_group.add_option("-L,--local", m_local_path, "Use a local directory as backend.")->type_name("PATH");
        backend_group.add_flag("-S,--sshfs,--sftp", m_sshfs_url, "Use libssh to connect to a server as backend.")->type_name("URL");

        m_cmd.add_flag("-d,--debug", "Enable FUSE debug output (implies -f)");
        m_cmd.add_flag("-f,--foreground", "Stay in foreground");

        m_cmd.add_option("cachedir", m_cachedir, "Path to the cache directory")->mandatory()->type_name("PATH");
        m_cmd.add_option("mountpoint", m_mountpoint, "Path to the mountpoint")->mandatory()->type_name("PATH");
    }

private:
    CLI::App &m_cmd;

    std::string m_cachedir;
    std::string m_mountpoint;
    std::string m_local_path;
    std::string m_sshfs_url;

public:
    int execute() {
        const bool debug = m_cmd.count("-d");
        const bool foreground = debug || m_cmd.count("-f");
        const bool clone_fd = true;

        std::unique_ptr<Dragonstash::Backend::Filesystem> backend;
        if (m_cmd.count("--disconnected")) {
            auto in_memory = std::make_unique<Dragonstash::Backend::InMemoryFilesystem>();
            in_memory->set_connected(false);
            backend = std::move(in_memory);
        } else if (m_cmd.count("--local")) {
            backend = std::make_unique<Dragonstash::Backend::LocalFilesystem>(std::filesystem::path(m_local_path));
        }
        Dragonstash::Cache cache(m_cachedir);
        Dragonstash::Filesystem fs(cache, *backend);

        // construct an argv array to trick fuse into setting the right options
        // ... this is a bit hacky, but it does what's needed.
        std::vector<std::string> shadow_argv;
        shadow_argv.emplace_back("\0", 1);
        if (debug) {
            shadow_argv.emplace_back("-d");
        }

        std::vector<char*> argv;
        argv.reserve(shadow_argv.size());
        for (auto &s: shadow_argv) {
            argv.push_back(s.data());
        }

        struct fuse_args args{static_cast<int>(argv.size()), argv.data(), 0};

        int ret = 255;
        Fuse::Session<Dragonstash::Filesystem> session(fs, &args);

        if (session.set_signal_handlers() != 0) {
            std::cerr << "failed to set signal handlers" << std::endl;
            ret = 1;
            goto exit;
        }

        if (session.mount(m_mountpoint.c_str()) != 0) {
            std::cerr << "failed to mount" << std::endl;
            ret = 1;
            goto cleanup_signal;
        }

        fuse_daemonize(foreground);

        ret = !session.loop_mt(clone_fd);

        session.unmount();

cleanup_signal:
        session.remove_signal_handlers();
exit:
        return ret;
    }

    explicit operator bool() const {
        return bool(m_cmd);
    }

};


int main(int argc, char **argv) {
    CLI::App app{"Dragonstash"};

    MountCommand mount(app);

    CLI11_PARSE(app, argc, argv);

    if (mount) {
        mount.execute();
    }
    return 0;
}
