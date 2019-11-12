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
#include "fuse/request.hpp"
#include "fuse/buffer.hpp"
#include "fuse/interface.hpp"
#include "fs.hpp"

#include <iostream>
#include <cstring>

#include "local_backend.hpp"

int main(int argc, char **argv) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_cmdline_opts opts;

    int ret = 1;
    if (fuse_parse_cmdline(&args, &opts) != 0)
            return 1;
    if (opts.show_help) {
            printf("usage: %s [options] <mountpoint>\n\n", argv[0]);
            fuse_cmdline_help();
            fuse_lowlevel_help();
            ret = 0;
            goto err_out1;
    } else if (opts.show_version) {
            printf("FUSE library version %s\n", fuse_pkgversion());
            fuse_lowlevel_version();
            ret = 0;
            goto err_out1;
    }

    if(opts.mountpoint == NULL) {
            printf("usage: %s [options] <mountpoint>\n", argv[0]);
            printf("       %s --help\n", argv[0]);
            ret = 1;
            goto err_out1;
    }

    {
        const std::filesystem::path cache_path = "./cache";
        const std::filesystem::path src_path = "/home/horazont/noram-tmp";
        Dragonstash::Backend::LocalFilesystem backend(src_path);
        Dragonstash::Cache cache(cache_path);
        Dragonstash::Filesystem fs(cache, backend);
        try {
            Fuse::Session<Dragonstash::Filesystem> session(fs, &args);
            if (session.set_signal_handlers() != 0) {
                std::cerr << "failed to set signal handlers" << std::endl;
                ret = 1;
                goto err_out2;
            }

            if (session.mount(opts.mountpoint) != 0) {
                std::cerr << "failed to mount" << std::endl;
                ret = 1;
                goto err_out3;
            }

            fuse_daemonize(opts.foreground);

            if (opts.singlethread && false) {
                ret = !session.loop();
            } else {
                ret = !session.loop_mt(opts.clone_fd);
            }

            session.unmount();
err_out3:
            session.remove_signal_handlers();
err_out2:
            ;
        } catch (std::runtime_error &exc) {
            std::cerr << "failed to set up session" << std::endl;
        }
    }

err_out1:
    fuse_opt_free_args(&args);

    return ret;
}
