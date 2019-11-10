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

class HelloFilesystem: public Fuse::Interface {
private:
    static const std::string hello_name;
    static const std::string hello_str;

private:
    int hello_stat(fuse_ino_t ino, struct stat &stbuf) {
        stbuf.st_ino = ino;
        switch (ino) {
        case 1:
            stbuf.st_mode = S_IFDIR | 0755;
            stbuf.st_nlink = 2;
            break;
        case 2:
            stbuf.st_mode = S_IFREG | 0644;
            stbuf.st_nlink = 1;
            stbuf.st_size = hello_str.size();
            break;
        default:
            return EBADF;
        }

        return 0;
    }

    int reply_buf_sliced(Fuse::Request &&req,
                         const std::basic_string<char> &buf,
                         const off_t off,
                         const size_t maxsize)
    {
        if (off < buf.size()) {
            size_t sz = buf.size() - off;
            if (sz > maxsize) {
                sz = maxsize;
            }
            return req.reply_buf(buf.data() + off, sz);
        }
        return req.reply_buf(nullptr, 0);
    }

public:
    void lookup(Fuse::Request &&req, fuse_ino_t parent, const char *name) {
        struct fuse_entry_param e;
        if (parent != 1 || name != hello_name) {
            req.reply_err(ENOENT);
            return;
        }

        memset(&e, 0, sizeof(e));
        e.ino = 2;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        hello_stat(e.ino, e.attr);
        req.reply_entry(&e);
    }

    void getattr(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
        struct stat stbuf;
        memset(&stbuf, 0, sizeof(stbuf));
        const int err = hello_stat(ino, stbuf);
        if (err != 0) {
            req.reply_err(errno);
        }
        req.reply_attr(stbuf, 1.0);
    }

    void open(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
        switch (ino) {
        case 1:
            req.reply_err(EISDIR);
            return;
        case 2:
            break;
        default:
            req.reply_err(EBADF);
            return;
        }

        if ((fi->flags) & O_ACCMODE != O_RDONLY) {
            req.reply_err(EACCES);
            return;
        }

        req.reply_open(fi);
    }

    void read(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off,
              struct fuse_file_info *fi) {
        if (ino != 2) {
            req.reply_err(EBADF);
            return;
        }

        reply_buf_sliced(std::move(req), hello_str, off, size);
    }

    void readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off,
                 struct fuse_file_info *fi) {
        if (ino != 1) {
            req.reply_err(ENOTDIR);
            return;
        }

        Fuse::DirBufferPlus buf;
        struct fuse_entry_param e;
        memset(&e, 0, sizeof(e));
        e.ino = 2;
        e.attr_timeout = 1.0;
        e.entry_timeout = 1.0;
        hello_stat(2, e.attr);
        buf.add(req, hello_name.c_str(), e);
        reply_buf_sliced(std::move(req), buf.get(), off, size);
    }
};

const std::string HelloFilesystem::hello_name = "hello";
const std::string HelloFilesystem::hello_str = "Hello World!\n";

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
        auto local = std::make_unique<Dragonstash::Backend::LocalFilesystem>(src_path);
        Dragonstash::Filesystem fs(std::make_unique<Dragonstash::Cache>(cache_path));
        fs.reset_backend_fs(std::move(local));
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

            if (opts.singlethread) {
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
