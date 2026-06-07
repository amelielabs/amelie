
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
#include <amelie_cdc.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_checkpoint.h>

void
checkpoint_mgr_init(CheckpointMgr* self)
{
	self->last       = NULL;
	self->list_count = 0;
	list_init(&self->list);
}

void
checkpoint_mgr_free(CheckpointMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto cp = list_at(Checkpoint, link);
		checkpoint_free(cp);
	}
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
checkpoint_mgr_open_dir(CheckpointMgr* self)
{
	// create directory
	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/checkpoints", state_directory());
	if (! fs_exists("{s}", path))
		fs_mkdir(0755, "{s}", path);

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
		state_lsn_follow(lsn);

		if (incomplete)
		{
			info("checkpoint: removing incomplete checkpoint: '{s}/{s}'",
			     path, entry->d_name);
			fs_rmdir("{s}/{s}", path, entry->d_name);
			continue;
		}
		auto cp = checkpoint_allocate(lsn);
		checkpoint_mgr_add(self, cp);
	}
}

void
checkpoint_mgr_open(CheckpointMgr* self)
{
	// get a list of available checkpoints
	checkpoint_mgr_open_dir(self);

	// set last checkpoint
	if (self->last)
		opt_int_set(&state()->checkpoint, self->last->id);
}

void
checkpoint_mgr_gc(CheckpointMgr* self)
{
	list_foreach_safe(&self->list)
	{
		auto cp = list_at(Checkpoint, link);
		if (cp == self->last)
			continue;
		if (cp->refs > 1)
			continue;

		// todo: rename as incomplete
		fs_rmdir("{s}/checkpoints/{u64}", state_directory(), cp->id);
		list_unlink(&cp->link);
		checkpoint_free(cp);
	}
}

void
checkpoint_mgr_add(CheckpointMgr* self, Checkpoint* cp)
{
	list_append(&self->list, &cp->link);
	self->list_count++;

	if (!self->last || self->last->id < cp->id)
		self->last = cp;
}
