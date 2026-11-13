
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

#include <amelie_runtime>
#include <amelie_type.h>
#include <amelie_page.h>

void
checkpoint_init(Checkpoint* self)
{
	self->lsn      = 0;
	self->path     = NULL;
	self->id       = NULL;
	self->schema   = NULL;
	self->page_mgr = NULL;
}

void
checkpoint_free(Checkpoint* self)
{
	unused(self);
}

void
checkpoint_prepare(Checkpoint* self, PageMgr* page_mgr, Uuid* id, Buf* schema)
{
	self->lsn      = state_lsn();
	self->path     = NULL;
	self->id       = id;
	self->schema   = schema;
	self->page_mgr = page_mgr;
}

static void
checkpoint_create_schema(Checkpoint* self)
{
	if (! self->schema)
		return;

	// create /storage/<uuid>/<lsn>.sql
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/{u64}.sql", self->path, self->lsn);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_WRONLY, 0644);
	if (buf_empty(self->schema))
		return;
	file_write_buf(&file, self->schema);
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);
}

static void
checkpoint_create(Checkpoint* self)
{
	// create /storage/<uuid>/<lsn>.incomplete
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/{u64}.incomplete",
	       self->path, self->lsn);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_WRONLY, 0644);

	auto page_mgr = self->page_mgr;
	for (auto i = 0; i < page_mgr->count; i++)
	{
		auto page = page_mgr_at(page_mgr, i);
		if (page->free)
			continue;
		page_save(page, &file);
	}

	// sync
	if (opt_int_of(&config()->storage_sync))
		file_sync(&file);

	// rename as completed
	fs_rename(path, "{s}/{u64}", self->path, self->lsn);

	// done
	double total = file.size / 1024.0 / 1024.0;
	info("checkpoint: {s}/{u64} ({.2f} mb)",
	     self->path, self->lsn, total);
}

static void
checkpoint_abort(Checkpoint* self)
{
	fs_unlink_if_exists("{s}/{u64}.sql", self->path, self->lsn);
	fs_unlink_if_exists("{s}/{u64}.incomplete", self->path, self->lsn);
}

bool
checkpoint(Checkpoint* self)
{
	// set directory
	char uuid[UUID_SZ];
	char path[PATH_MAX];
	uuid_get(self->id, uuid, sizeof(uuid));
	format(path, sizeof(path), "{s}/storage/{s}",
	       state_directory(), uuid);
	self->path = path;

	// create /storage/<uuid>, if not exists
	if (! fs_exists("{s}", path))
	{
		fs_mkdir(0755, "{s}", path);
		fs_syncdir("{s}/storage", state_directory());
	}

	// checkpoint already exists
	if (fs_exists("{s}/{u64}", path, self->lsn))
		return true;

	auto on_error = error_catch
	(
		// create lsn.sql
		checkpoint_create_schema(self);

		// create lsn.incomplete
		checkpoint_create(self);

		// sync directory
		if (opt_int_of(&config()->storage_sync))
			fs_syncdir("{s}", path);
	);
	if (on_error)
		checkpoint_abort(self);

	return !on_error;
}
