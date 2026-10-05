
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
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

typedef struct CheckpointPage CheckpointPage;
typedef struct Checkpoint     Checkpoint;

struct CheckpointPage
{
	Id    id;
	Page* snapshot;
};

struct Checkpoint
{
	uint64_t lsn;
	Buf      list;
	Buf      schema;
};

static inline void
checkpoint_init(Checkpoint* self, uint64_t lsn)
{
	self->lsn = lsn;
	buf_init(&self->list);
	buf_init(&self->schema);
}

static inline void
checkpoint_free(Checkpoint* self)
{
	buf_free(&self->list);
	buf_free(&self->schema);
}

static inline void
checkpoint_add(Checkpoint* self, Page* page, int page_fd)
{
	auto ref = (CheckpointPage*)buf_emplace(&self->list, sizeof(CheckpointPage));
	memcpy(&ref->id, &page->id, sizeof(ref->id));

	// create page snapshot, if it has pending changes
	ref->snapshot = NULL;
	if (page->changed)
	{
		page->changed = false;
		ref->snapshot = page_allocate_snapshot(page, page_fd);
	}
}

hot static inline void
checkpoint_add_storage(Checkpoint* self, Storage* storage)
{
	checkpoint_add(self, storage->meta, storage->meta_fd);
	for (auto i = 0; i < storage->list_count; i++)
		checkpoint_add(self, storage_at(storage, i), *storage_at_fd(storage, i));
}

static void
checkpoint_create(Checkpoint* self)
{
	// create checkpoint/<lsn>.incomplete
	info("");
	info("checkpoint: checkpoint/{u64}", self->lsn);
	fs_mkdir(0755, "{s}/checkpoint/{u64}.incomplete",
	         state_directory(), self->lsn);

	// create schema.sql
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete/schema.sql",
	       state_directory(), self->lsn);

	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0644);
	if (! buf_empty(&self->schema))
	{
		file_write_buf(&file, &self->schema);
		if (opt_int_of(&config()->storage_sync))
			file_sync(&file);
	}

	// create partition files
	auto pos = (CheckpointPage*)self->list.start;
	auto end = (CheckpointPage*)self->list.position;
	for (; pos < end; pos++)
	{
		// create page hardlink using previous checkpoint
		if (! pos->snapshot)
		{
			char path_prev[PATH_MAX];
			id_path(&pos->id, path_prev, state_checkpoint(), false);
			id_path(&pos->id, path, self->lsn, true);
			auto rc = link(path_prev, path);
			if (rc == -1)
				error_system();
			continue;
		}

		// create page file
		page_save(pos->snapshot, self->lsn);

		// free snapshot as soon as possible
		page_free_snapshot(pos->snapshot);
		pos->snapshot = NULL;

		// todo: info
	}

	format(path, sizeof(path), "{s}/checkpoint/{u64}.incomplete",
	       state_directory(), self->lsn);

	// sync checkpoint dir
	if (opt_int_of(&config()->storage_sync))
		fs_syncdir("{s}", path);

	// sync checkpoint base dir
	if (opt_int_of(&config()->storage_sync))
		fs_syncdir("{s}/checkpoint", state_directory());

	// rename as completed
	fs_rename(path, "{s}/checkpoint/{u64}", state_directory(),
	          self->lsn);

	// done
	info("");
}

static void
checkpoint_abort(Checkpoint* self)
{
	auto pos = (CheckpointPage*)self->list.start;
	auto end = (CheckpointPage*)self->list.position;
	for (; pos < end; pos++)
	{
		if (pos->snapshot)
		{
			page_free_snapshot(pos->snapshot);
			pos->snapshot = NULL;
		}
	}

	fs_rmdir("{s}/checkpoint/{u64}.incomplete", state_directory(),
	         self->lsn);

	error("checkpoint: {u64} failed", self->lsn);
}

static void
checkpoint_job(intptr_t* argv)
{
	auto self = (Checkpoint*)argv[0];
	auto on_error = error_catch (
		checkpoint_create(self);
	);
	if (on_error)
	{
		checkpoint_abort(self);
		rethrow();
	}
}

static void
checkpoint_prepare(Checkpoint* self, Catalog* catalog)
{
	// create schema.sql content
	describe_catalog(catalog, &self->schema);

	// collect pages
	list_foreach(&catalog->rels.list)
	{
		auto rel = list_at(Rel, link);
		if (rel->type != REL_TABLE)
			continue;
		auto table = table_of(rel);
		table_sync(table);

		list_foreach(&table->parts.list)
		{
			// heap
			auto part = list_at(Part, link);
			checkpoint_add_storage(self, &part->heap->storage);

			// vector stores
			auto pos = (Flat**)part->flats.list.start;
			auto end = (Flat**)part->flats.list.position;
			for (; pos < end; pos++)
				checkpoint_add_storage(self, &(*pos)->storage);
		}
	}
}

void
checkpoint(Checkpoints* checkpoints, Catalog* catalog)
{
	if (state_lsn() == state_checkpoint())
		return;

	// take checkpoint lock (one checkpoint, create index or backup at a time)
	auto checkpoint_lock = lock_system(REL_CHECKPOINT, LOCK_EXCLUSIVE);
	defer(unlock, checkpoint_lock);

	// take exclusive catalog lock
	auto catalog_lock = lock_system(REL_CATALOG, LOCK_EXCLUSIVE);

	Checkpoint cp;
	checkpoint_init(&cp, state_lsn());
	defer(checkpoint_free, &cp);
	auto on_error =
		error_catch(checkpoint_prepare(&cp, catalog));

	// unlock catalog (still keeping checkpoint lock)
	unlock(catalog_lock);

	if (on_error)
		rethrow();

	// run and wait
	run(checkpoint_job, 1, &cp);

	// set checkpoint
	checkpoints_add(checkpoints, cp.lsn);
}
