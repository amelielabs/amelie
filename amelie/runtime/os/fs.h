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

static inline bool format_validate(1, 2)
fs_exists(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path) >= 0;
}

static inline void format_validate(2, 3)
fs_mkdir(int mode, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_mkdir(path, mode);
	if (unlikely(rc == -1))
		error_system();
}

static inline void format_validate(1, 2)
fs_unlink(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_unlink(path);
	if (unlikely(rc == -1))
		error_system();
}

static inline void format_validate(2, 3)
fs_rename(const char* old, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_rename(old, path);
	if (unlikely(rc == -1))
		error_system();
}

static inline int64_t format_validate(1, 2)
fs_size(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path);
}

static void
fs_opendir_defer(DIR* self)
{
	closedir(self);
}

static inline void format_validate(2, 3)
fs_rmdir(bool with_directory, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsfmt(path, sizeof(path), fmt, args);
	va_end(args);
	if (! fs_exists("%s", path))
		return;
	auto dir = opendir(path);
	if (unlikely(! dir))
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
	if (with_directory)
	{
		if (vfs_rmdir(path) == -1)
			error_system();
	}
}
