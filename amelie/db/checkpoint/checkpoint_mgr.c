
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_heap.h>
#include <amelie_cdc.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>

static inline CheckpointRef*
checkpoint_ref_allocate(uint64_t id)
{
	auto self = (CheckpointRef*)am_malloc(sizeof(CheckpointRef));
	self->id   = id;
	self->refs = 0;
	list_init(&self->link);
	return self;
}

static inline void
checkpoint_ref_free(CheckpointRef* self)
{
	am_free(self);
}

void
checkpoint_mgr_init(CheckpointMgr* self, Catalog* catalog)
{
	self->current    = NULL;
	self->list_count = 0;
	self->catalog    = catalog;
	list_init(&self->list);
	spinlock_init(&self->lock);
}

void
checkpoint_mgr_free(CheckpointMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto cp = list_at(CheckpointRef, link);
		checkpoint_ref_free(cp);
	}
	spinlock_free(&self->lock);
}

static inline int64_t
checkpoint_id_of(const char* name, bool* incomplete)
{
	// <lsn>[.incomplete]
	int64_t lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(&lsn, lsn, 10, *name - '0')))
			return -1;
		name++;
	}
	*incomplete = (*name == '.') && !strcmp(name, ".incomplete");
	return lsn;
}

static void
checkpoint_mgr_read(CheckpointMgr* self)
{
	// create directory
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoints", state_directory());

	// read directory, get a list of checkpoints
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("checkpoint: directory '{s}' open error", path);
	defer(fs_closedir_defer, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		bool incomplete = false;
		auto lsn = checkpoint_id_of(entry->d_name, &incomplete);
		if (lsn == -1)
			continue;

		if (incomplete)
		{
			info("checkpoint: removing incomplete checkpoint: '{s}/{s}'",
			     path, entry->d_name);
			fs_rmdir("{s}/{s}", path, entry->d_name);
			continue;
		}
		checkpoint_mgr_add(self, lsn);
	}
}

void
checkpoint_mgr_open(CheckpointMgr* self)
{
	// get a list of available checkpoints
	checkpoint_mgr_read(self);

	// set last checkpoint
	if (! self->current)
		error("checkpoint: no checkpoints found");

	auto id = self->current->id;
	state_checkpoint_set(id);
	state_lsn_follow(id);

	// restore last checkpoint
	char path[PATH_MAX];
	format(path, sizeof(path),
	       "{s}/checkpoints/{u64}/catalog.json",
	       state_directory(),
	       state_checkpoint());

	catalog_read(self->catalog, path);
}

void
checkpoint_mgr_gc(CheckpointMgr* self)
{
	for (;;)
	{
		CheckpointRef* gc = NULL;

		spinlock_lock(&self->lock);
		list_foreach_safe(&self->list)
		{
			auto ref = list_at(CheckpointRef, link);
			if (ref == self->current)
				continue;
			if (ref->refs > 0)
				continue;

			gc = ref;
			list_unlink(&ref->link);
			break;

		}
		spinlock_unlock(&self->lock);
		if (! gc)
			break;

		defer(checkpoint_ref_free, gc);

		// rename as incomplete
		char path[PATH_MAX];
		format(path, sizeof(path), "{s}/checkpoints/{u64}", state_directory(), gc->id);
		fs_rename(path, "{s}/checkpoints/{u64}.incomplete",
		          state_directory(), gc->id);

		// remove
		fs_rmdir("{s}/checkpoints/{u64}.incomplete", state_directory(), gc->id);
	}
}

void
checkpoint_mgr_add(CheckpointMgr* self, uint64_t lsn)
{
	auto ref = checkpoint_ref_allocate(lsn);
	spinlock_lock(&self->lock);

	list_append(&self->list, &ref->link);
	self->list_count++;

	if (!self->current || self->current->id < ref->id)
	{
		self->current = ref;
		state_checkpoint_set(lsn);
	}

	spinlock_unlock(&self->lock);
}

CheckpointRef*
checkpoint_mgr_ref(CheckpointMgr* self)
{
	spinlock_lock(&self->lock);
	auto current = self->current;
	assert(current);
	current->refs++;
	spinlock_unlock(&self->lock);

	return current;
}

void
checkpoint_mgr_unref(CheckpointMgr* self, CheckpointRef *ref)
{
	spinlock_lock(&self->lock);
	ref->refs--;
	assert(ref->refs >= 0);
	spinlock_unlock(&self->lock);
}

static inline int64_t
part_id_of(const char* name)
{
	int64_t id = 0;
	while (*name)
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(&id, id, 10, *name - '0')))
			return -1;
		name++;
	}
	return id;
}

void
checkpoint_mgr_list(CheckpointRef* ref, Buf* buf)
{
	char path_relative[PATH_MAX];
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoints/{u64}",
	       state_directory(), ref->id);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_closedir_defer, dir);

	encode_array(buf);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (! strcmp(entry->d_name, "."))
			continue;
		if (! strcmp(entry->d_name, ".."))
			continue;
		auto id = part_id_of(entry->d_name);
		if (id == -1)
			continue;
		format(path_relative, sizeof(path_relative),
		       "checkpoints/{u64}/{s}", ref->id, entry->d_name);
		encode_basefile(buf, path_relative);
	}

	// catalog.json
	format(path_relative, sizeof(path_relative),
	       "checkpoints/{u64}/catalog.json", ref->id);
	encode_basefile(buf, path_relative);

	encode_array_end(buf);
}
