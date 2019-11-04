# DragonStash -- a Caching FUSE Overlay File System

`DragonStash` is a FUSE file system which implements a *transparent cache*
over any other mounted file system or SFTP server. It helps you produce a
Dragon’s stash of the finest media (or whatever is available at the source
you’re using), automatically.

## Roadmap

`DragonStash` is in development. This means that a lot of things don’t work
yet. Here’s the roadmap and the current status.

### Done

* Some internals.

### To be done

Everything, literally. But more specifically:

* Transparent caching of inodes (directories, symlinks, file metadata).
* Transparent block-wise caching of file contents
* Local directory tree as source file system
* SFTP server as source file system
* -EIO on missing data
* Limit number of blocks and inodes in the cache
* Evict unused blocks when limit is reached
* Understand fallocate to discard cached data
* Proper command-line interface and utility for:

  - mounting and unmounting
  - pinning file content, configuring readahead etc.

* Online write support
* Offline write support
* Online locking support
* (Unsafe) offline locking support
* More advanced readahead/pre-caching algorithms

### Out of scope

* Sockets, devices, fifos

## Huh? Wasn't this in golang once?

[Yes](https://github.com/horazont/dragonstash-golang). I got annoyed over
golang too much to continue it there. So I ported it to a saner language
(C++).

(Yes, I am a bit provocative about the above statement. In the end, it is
about a matter of taste.)
