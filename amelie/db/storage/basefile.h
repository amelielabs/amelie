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

static inline void
encode_basepath(Buf* self, char* path_relative)
{
	// <base>/<path_relative>
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%s", state_directory(),
	     path_relative);
	encode_cstr(self, path_relative);
}

static inline void
decode_basepath(uint8_t** pos, Str* path_relative)
{
	// path_relative
	unpack_string(pos, path_relative);
}

static inline void
encode_basefile(Buf* self, char* path_relative)
{
	// <base>/<path_relative>
	char path[PATH_MAX];
	sfmt(path, sizeof(path), "%s/%s", state_directory(),
	     path_relative);

	struct stat st;
	if (lstat(path, &st) == -1)
		error_system();

	// []
	encode_array(self);

	// path_relative
	encode_cstr(self, path_relative);

	// size
	encode_int(self, st.st_size);

	// mode
	encode_int(self, st.st_mode);
	encode_array_end(self);
}

static inline void
decode_basefile(uint8_t** pos, Str* path_relative, int64_t* size, int64_t* mode)
{
	// []
	unpack_array(pos);

	// path_relative
	unpack_string(pos, path_relative);

	// size
	unpack_int(pos, size);

	// mode
	unpack_int(pos, mode);
	unpack_array_end(pos);
}
