#pragma once

//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// Copyright (c) 2024 Amelie Labs.
//
// AGPL-3.0 Licensed.
//

int     vfs_unlink(const char*);
int     vfs_rename(const char*, const char*);
int     vfs_mkdir(const char*, int);
int     vfs_rmdir(const char*);
int     vfs_open(const char*, int, int);
int     vfs_close(int);
int     vfs_flock_shared(int);
int     vfs_flock_exclusive(int);
int     vfs_flock_unlock(int);
int     vfs_fdatasync(int);
int     vfs_fsync(int);
int     vfs_sync_file_range(int, uint64_t, uint64_t);
int     vfs_truncate(int, uint64_t);
int64_t vfs_read_raw(int, void*, uint64_t);
int64_t vfs_read(int, void*, uint64_t);
int64_t vfs_pread(int, uint64_t, void*, uint64_t);
int64_t vfs_write(int, void*, uint64_t);
int64_t vfs_pwrite(int, void*, uint64_t, uint64_t);
int64_t vfs_writev(int, struct iovec*, int);
int64_t vfs_pwritev(int, uint64_t, struct iovec*, int);
int64_t vfs_seek(int, uint64_t);
int64_t vfs_size_fd(int);
int64_t vfs_size(char*);
void   *vfs_mmap(int, uint64_t);
void   *vfs_mremap(void*, uint64_t, uint64_t);
int     vfs_munmap(void*, uint64_t);
