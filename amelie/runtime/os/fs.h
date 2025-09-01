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
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	struct statx stat;
	return io_statx(path, AT_STATX_SYNC_AS_STAT, STATX_MODE, &stat) >= 0;
}

static inline void
fs_mkdir(int mode, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	if (unlikely(io_mkdir(path, mode) == -1))
		error_system();
}

static inline void
fs_unlink(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	if (unlikely(io_unlink(path, 0) == -1))
		error_system();
}

static inline void
fs_rename(const char* old, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	if (unlikely(io_rename(old, path) == -1))
		error_system();
}

static inline ssize_t
fs_size(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	struct statx stat;
	stat.stx_size = 0;
	if (unlikely(io_statx(path, AT_STATX_SYNC_AS_STAT, STATX_SIZE, &stat) == -1))
		error_system();
	return stat.stx_size;
}

static void
fs_opendir_defer(DIR* self)
{
	closedir(self);
}

static inline void
fs_rmdir(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_opendir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		fs_unlink("%s/%s", path, entry->d_name);
	}
	if (unlikely(io_unlink(path, AT_REMOVEDIR) == -1))
		error_system();
}
