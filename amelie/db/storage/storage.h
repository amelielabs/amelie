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

typedef struct Storage Storage;

struct Storage
{
	Relation       rel;
	StorageConfig* config;
	int            refs;
	List           link;
};

static inline void
storage_free(Storage* self)
{
	storage_config_free(self->config);
	am_free(self);
}

static inline Storage*
storage_allocate(StorageConfig* config)
{
	auto self = (Storage*)am_malloc(sizeof(Storage));
	self->config = storage_config_copy(config);
	self->refs   = 0;
	list_init(&self->link);
	relation_init(&self->rel);
	relation_set_db(&self->rel, NULL);
	relation_set_name(&self->rel, &self->config->name);
	relation_set_free_function(&self->rel, (RelationFree)storage_free);
	return self;
}

static inline void
storage_ref(Storage* self)
{
	self->refs++;
}

static inline void
storage_unref(Storage* self)
{
	self->refs--;
	assert(self->refs >= 0);
}

static inline Storage*
storage_of(Relation* self)
{
	return (Storage*)self;
}

static inline void
storage_fmt(Storage* self, char* buf, char* fmt, ...)
{
	// set relative path
	char relative[512];
	va_list args;
	va_start(args, fmt);
	vsfmt(relative, sizeof(relative), fmt, args);
	va_end(args);

	auto path = &self->config->path;
	if (*str_of(path) == '/')
	{
		// <absolute_path>/<relative>
		sfmt(buf, PATH_MAX, "%.*s/%s", str_size(path),
		     str_of(path), relative);
	} else
	{
		// <base>/<path>/<relative>
		sfmt(buf, PATH_MAX, "%s/%.*s/%s", state_directory(),
		     str_size(path), str_of(path), relative);
	}
}

static inline void
storage_path(Storage* self, char* buf, Id* id, int state)
{
	// tier id (uuid)
	char id_tier[UUID_SZ];
	uuid_get(&id->id_tier, id_tier, sizeof(id_tier));

	switch (state) {
	case ID:
		// <storage_path>/<id_tier>/<id_part>/<id>
		storage_fmt(self, buf,
		            "%s/%05" PRIu64 "/%05" PRIu64,
		            id_tier,
		            id->id_part,
		            id->id);
		break;
	case ID_INCOMPLETE:
		// <storage_path>/<id_tier>/<id_part>/<id>.incomplete
		storage_fmt(self, buf,
		            "%s/%05" PRIu64 "/%05" PRIu64 ".incomplete",
		            id_tier,
		            id->id_part,
		            id->id);
		break;
	default:
		abort();
		break;
	}
}

static inline void
storage_create(Storage* self, File* file, Id* id, int state)
{
	char path[PATH_MAX];
	storage_path(self, path, id, state);
	file_create(file, path);
}

static inline void
storage_open(Storage* self, File* file, Id* id, int state)
{
	char path[PATH_MAX];
	storage_path(self, path, id, state);
	file_open(file, path);
}

static inline void
storage_delete(Storage* self, Id* id, int state)
{
	// <source_path>/<table_uuid>/<id_parent>.<id>
	char path[PATH_MAX];
	storage_path(self, path, id, state);
	if (fs_exists("%s", path))
		fs_unlink("%s", path);
}

static inline void
storage_rename(Storage* self, Id* id, int from, int to)
{
	// rename file from one state to another
	char path_from[PATH_MAX];
	char path_to[PATH_MAX];
	storage_path(self, path_from, id, from);
	storage_path(self, path_to, id, to);
	if (fs_exists("%s", path_from))
		fs_rename(path_from, "%s", path_to);
}
