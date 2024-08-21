#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Remote Remote;

enum
{
	REMOTE_URI,
	REMOTE_PATH_CA,
	REMOTE_FILE_CA,
	REMOTE_FILE_CERT,
	REMOTE_FILE_KEY,
	REMOTE_SERVER,
	REMOTE_MAX
};

struct Remote
{
	Str options[REMOTE_MAX];
};

static inline void
remote_init(Remote* self)
{
	memset(self->options, 0, sizeof(self->options));
}

static inline void
remote_free(Remote* self)
{
	for (int i = 0; i < REMOTE_MAX;i ++)
		str_free(&self->options[i]);
}

static inline void
remote_set(Remote* self, int id, Str* value)
{
	str_free(&self->options[id]);
	str_copy(&self->options[id], value);
}

static inline void
remote_set_path(Remote* self, int id, const char* directory, Str* name)
{
	// relative to the cwd
	auto relative = str_compare_raw_prefix(name, "./", 2) ||
	                str_compare_raw_prefix(name, "../", 3);

	// absolute or relative file path
	if (*str_of(name) == '/' || relative)
	{
		remote_set(self, id, name);
		return;
	}

	// relative to the directory
	char path[PATH_MAX];
	int  path_size;
	path_size = snprintf(path, sizeof(path), "%s/%.*s", directory,
	                     str_size(name), str_of(name));
	Str dir_path;
	str_set(&dir_path, path, path_size);
	remote_set(self, id, &dir_path);
}

static inline Str*
remote_get(Remote* self, int id)
{
	return &self->options[id];
}

static inline char*
remote_get_cstr(Remote* self, int id)
{
	return str_of(&self->options[id]);
}

static inline void
remote_copy(Remote* self, Remote* from)
{
	for (int i = 0; i < REMOTE_MAX;i ++)
		if (! str_empty(&from->options[i]))
			remote_set(self, i, &from->options[i]);
}
