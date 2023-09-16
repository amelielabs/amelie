#pragma once

//
// monotone
//
// SQL OLTP database
//

static inline bool
fs_exists(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path) >= 0;
}

static inline void
fs_mkdir(int mode, const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
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
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	int rc = vfs_unlink(path);
	if (unlikely(rc == -1))
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
	int rc = vfs_rename(old, path);
	if (unlikely(rc == -1))
		error_system();
}

static inline int64_t
fs_size(const char* fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	char path[PATH_MAX];
	vsnprintf(path, sizeof(path), fmt, args);
	va_end(args);
	return vfs_size(path);
}
