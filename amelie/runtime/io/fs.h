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

static inline bool
fs_exists(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path) >= 0;
}

static inline void
fs_mkdir(int mode, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_mkdir(path, mode);
	if (unlikely(rc == -1))
		error_system();
}

static inline void
fs_unlink(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_unlink(path);
	if (unlikely(rc == -1))
		error_system();
}

static inline void
fs_unlink_if_exists(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_unlink(path);
	if (unlikely(rc == -1 && errno != ENOENT))
		error_system();
}

static inline void
fs_rename(const char* old, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_rename(old, path);
	if (unlikely(rc == -1))
		error_system();
}

static inline void
fs_syncdir(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);

	auto fd = vfs_open(path, O_RDONLY|O_DIRECTORY, 0);
	if (unlikely(fd == -1))
		error_system();

	auto rc = vfs_fsync(fd);
	if (rc == -1)
	{
		vfs_close(fd);
		error_system();
	}
	vfs_close(fd);
}

static inline void
fs_rename_exchange(const char* old, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = renameat2(AT_FDCWD, old, AT_FDCWD, path, RENAME_EXCHANGE);
	if (unlikely(rc == -1))
		error_system();
}

static inline int64_t
fs_size(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path);
}

static void
fs_closedir_defer(DIR* self)
{
	closedir(self);
}

static inline void
fs_rmdir(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	formatv(path, sizeof(path), fmt, args);
	va_end(args);
	if (! fs_exists("{s}", path))
		return;
	auto dir = opendir(path);
	if (unlikely(! dir))
		error_system();
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		fs_unlink("{s}/{s}", path, entry->d_name);
	}
	if (vfs_rmdir(path) == -1)
		error_system();
}
