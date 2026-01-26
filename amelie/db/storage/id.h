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

typedef struct Id   Id;
typedef struct Tier Tier;

enum
{
	ID_NONE            = 0,
	ID_HEAP            = 1 << 0,
	ID_HEAP_INCOMPLETE = 1 << 1
};

struct Id
{
	uint64_t  id;
	Uuid      id_tier;
	Uuid      id_table;
	int       state;
	Storage*  storage;
	Tier*     tier;
	Encoding* encoding;
} packed;

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
id_set(Id* self, int state)
{
	self->state |= state;
}

static inline void
id_unset(Id* self, int state)
{
	self->state &= ~state;
}

static inline bool
id_has(Id* self, int state)
{
	return (self->state & state) > 0;
}

static inline int
id_of(const char* name, int64_t* id)
{
	// <id>.heap
	// <id>.heap.incomplete
	*id = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(id, *id, 10, *name - '0')))
			return -1;
		name++;
	}
	int state = -1;
	if (! strcmp(name, ".heap.incomplete"))
		state = ID_HEAP_INCOMPLETE;
	else
	if (! strcmp(name, ".heap"))
		state = ID_HEAP;
	return state;
}

static inline void
id_path(Id* self, char* path, int state)
{
	// tier id (uuid)
	char id_tier[UUID_SZ];
	uuid_get(&self->id_tier, id_tier, sizeof(id_tier));

	switch (state) {
	case ID_HEAP:
		// <storage_path>/<id_tier>/<id>.heap
		storage_pathfmt(self->storage, path, "%s/%05" PRIu64 ".heap",
		                id_tier, self->id);
		break;
	case ID_HEAP_INCOMPLETE:
		// <storage_path>/<id_tier>/<id>.heap.incomplete
		storage_pathfmt(self->storage, path, "%s/%05" PRIu64 ".heap.incomplete",
		                id_tier, self->id);
		break;
	default:
		abort();
		break;
	}
}

static inline void
id_create(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	file_create(file, path);
}

static inline void
id_open(Id* self, File* file, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	file_open(file, path);
}

static inline void
id_delete(Id* self, int state)
{
	char path[PATH_MAX];
	id_path(self, path, state);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

static inline void
id_rename(Id* self, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	id_path(self, path_from, from);
	id_path(self, path_to, to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
