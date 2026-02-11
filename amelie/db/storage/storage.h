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
storage_free(Storage* self, bool drop)
{
	unused(drop);
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
storage_pathfmt(Storage* self, char* buf, char* fmt, ...)
{
	// set relative path
	char relative[512];
	va_list args;
	va_start(args, fmt);
	vsfmt(relative, sizeof(relative), fmt, args);
	va_end(args);

	// <base>/storage/<name>
	auto name = &self->config->name;
	sfmt(buf, PATH_MAX, "%s/storage/%.*s%s%s", state_directory(),
	     str_size(name), str_of(name),
	     *relative? "/" : "",
	      relative);
}

static inline void
storage_mkdir(Storage* self)
{
	// <base>/storage/<name>
	char path[PATH_MAX];
	storage_pathfmt(self, path, "", NULL);
	if (fs_exists("%s", path))
		return;

	// <base>/storage/<name> (directory)
	auto path_storage = &self->config->path;
	if (str_empty(path_storage))
	{
		fs_mkdir(0755, "%s", path);
		return;
	}

	// create storage directory, if not exists
	if (! fs_exists("%s", str_of(path_storage)))
		fs_mkdir(0755, "%s", str_of(path_storage));

	// <base>/storage/<name> -> /storage/path (symlink)
	auto rc = symlink(str_of(path_storage), path);
	if (rc == -1)
		error_system();
}
