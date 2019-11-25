/**********************************************************************
File name: interface.cpp
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
#include "dragonstash/fuse/interface.hpp"

namespace Fuse {

void Interface::init(fuse_conn_info *conn)
{

}

void Interface::destroy()
{

}

/* generated using:
 * sed -r 's/\s+void (\w+)\([^)]+)\);/void FuseInterface::\1(\2) {\n    req.reply_err(ENOSYS);\n}\n/' < calls
 */

void Interface::lookup(Fuse::Request &&req, fuse_ino_t parent, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::forget(Fuse::Request &&req, fuse_ino_t ino, uint64_t nlookup) {
    req.reply_err(ENOSYS);
}

void Interface::getattr(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::setattr(Fuse::Request &&req, fuse_ino_t ino, struct stat &attr, int to_set, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::readlink(Fuse::Request &&req, fuse_ino_t ino) {
    req.reply_err(ENOSYS);
}

void Interface::mknod(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode, dev_t rdev) {
    req.reply_err(ENOSYS);
}

void Interface::mkdir(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode) {
    req.reply_err(ENOSYS);
}

void Interface::unlink(Fuse::Request &&req, fuse_ino_t parent, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::rmdir(Fuse::Request &&req, fuse_ino_t parent, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::symlink(Fuse::Request &&req, const char *link, fuse_ino_t parent, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::rename(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, fuse_ino_t newparent, std::string_view newname, unsigned int flags) {
    req.reply_err(ENOSYS);
}

void Interface::link(Fuse::Request &&req, fuse_ino_t ino, fuse_ino_t newparent, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::open(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::read(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::write(Fuse::Request &&req, fuse_ino_t ino, std::string_view buf, off_t off, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::flush(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::release(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::fsync(Fuse::Request &&req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::opendir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::readdir(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::releasedir(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::fsyncdir(Fuse::Request &&req, fuse_ino_t ino, int datasync, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::statfs(Fuse::Request &&req, fuse_ino_t ino) {
    req.reply_err(ENOSYS);
}

void Interface::setxattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name, std::string_view value, int flags) {
    req.reply_err(ENOSYS);
}

void Interface::getxattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name, size_t size) {
    req.reply_err(ENOSYS);
}

void Interface::listxattr(Fuse::Request &&req, fuse_ino_t ino, size_t size) {
    req.reply_err(ENOSYS);
}

void Interface::removexattr(Fuse::Request &&req, fuse_ino_t ino, std::string_view name) {
    req.reply_err(ENOSYS);
}

void Interface::access(Fuse::Request &&req, fuse_ino_t ino, int mask) {
    req.reply_err(ENOSYS);
}

void Interface::create(Fuse::Request &&req, fuse_ino_t parent, std::string_view name, mode_t mode, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::getlk(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock) {
    req.reply_err(ENOSYS);
}

void Interface::setlk(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct flock *lock, int sleep) {
    req.reply_err(ENOSYS);
}

void Interface::bmap(Fuse::Request &&req, fuse_ino_t ino, size_t blocksize, uint64_t idx) {
    req.reply_err(ENOSYS);
}

void Interface::ioctl(Fuse::Request &&req, fuse_ino_t ino, unsigned int cmd, void *arg, struct fuse_file_info *fi, unsigned flags, const void *in_buf, size_t in_bufsz, size_t out_bufsz) {
    req.reply_err(ENOSYS);
}

void Interface::poll(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, struct fuse_pollhandle *ph) {
    req.reply_err(ENOSYS);
}

void Interface::write_buf(Fuse::Request &&req, fuse_ino_t ino, struct fuse_bufvec *bufv, off_t offset, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::retrieve_reply(Fuse::Request &&req, void *cookie, fuse_ino_t ino, off_t offset, struct fuse_bufvec *bufv) {
    req.reply_err(ENOSYS);
}

void Interface::forget_multi(Fuse::Request &&req, size_t count, struct fuse_forget_data *forgets) {
    req.reply_err(ENOSYS);
}

void Interface::flock(Fuse::Request &&req, fuse_ino_t ino, struct fuse_file_info *fi, int op) {
    req.reply_err(ENOSYS);
}

void Interface::fallocate(Fuse::Request &&req, fuse_ino_t ino, int mode, off_t offset, off_t length, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::readdirplus(Fuse::Request &&req, fuse_ino_t ino, size_t size, off_t off, struct fuse_file_info *fi) {
    req.reply_err(ENOSYS);
}

void Interface::copy_file_range(Fuse::Request &&req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info *fi_in, fuse_ino_t ino_out, off_t off_out, struct fuse_file_info *fi_out, size_t len, int flags) {
    req.reply_err(ENOSYS);
}

}
