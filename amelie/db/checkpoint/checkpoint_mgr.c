
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>

void
checkpoint_mgr_init(CheckpointMgr* self,
                    CheckpointIf*  iface,
                    void*          iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	id_mgr_init(&self->list);
	id_mgr_init(&self->list_snapshot);
}

void
checkpoint_mgr_free(CheckpointMgr* self)
{
	id_mgr_free(&self->list);
	id_mgr_free(&self->list_snapshot);
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
	snprintf(path, sizeof(path), "%s/checkpoints", state_directory());
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// read directory, get a list of checkpoints
	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error("checkpoint: directory '%s' open error", path);
	defer(fs_opendir_defer, dir);
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
			info("checkpoint: removing incomplete checkpoint: '%s/%s'",
			     path, entry->d_name);
			fs_rmdir("%s/%s", path, entry->d_name);
			continue;
		}
		checkpoint_mgr_add(self, lsn);
		state_lsn_follow(lsn);
	}

	// set last checkpoint
	auto last = id_mgr_max(&self->list);
	if (last != UINT64_MAX)
		opt_int_set(&state()->checkpoint, last);
}

static void
checkpoint_mgr_open_catalog(CheckpointMgr* self)
{
	auto checkpoint = state_checkpoint();
	if (! checkpoint)
		return;

	// read and parse catalog content
	auto buf = file_import("%s/checkpoints/%" PRIu64 "/catalog.json",
	                       state_directory(),
	                       checkpoint);
	defer_buf(buf);

	Str catalog_data;
	buf_str(buf, &catalog_data);

	Json json;
	json_init(&json);
	defer(json_free, &json);
	json_parse(&json, &catalog_data, NULL);

	// restore system objects
	uint8_t* pos = json.buf->start;
	self->iface->catalog_restore(&pos, self->iface_arg);
}

void
checkpoint_mgr_open(CheckpointMgr* self)
{
	// get a list of available checkpoints
	checkpoint_mgr_open_dir(self);

	// restore last checkpoint catalog
	checkpoint_mgr_open_catalog(self);
}

void
checkpoint_mgr_gc(CheckpointMgr* self)
{
	uint64_t min = state_checkpoint();
	uint64_t snapshot_min = id_mgr_min(&self->list_snapshot);
	if (snapshot_min < min)
		min = snapshot_min;

	// remove checkpoints < min
	Buf list;
	buf_init(&list);
	defer_buf(&list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);
	if (list_count > 0)
	{
		auto lsns = buf_u64(&list);
		for (int i = 0; i < list_count; i++)
			fs_rmdir("%s/checkpoints/%" PRIu64, state_directory(), lsns[i]);
	}
}

void
checkpoint_mgr_add(CheckpointMgr* self, uint64_t id)
{
	id_mgr_add(&self->list, id);
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
checkpoint_mgr_list(CheckpointMgr* self, uint64_t checkpoint, Buf* buf)
{
	unused(self);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/checkpoints/%" PRIu64,
	         state_directory(), checkpoint);

	auto dir = opendir(path);
	if (unlikely(dir == NULL))
		error_system();
	defer(fs_opendir_defer, dir);

	// read partitions to create a sorted list
	IdMgr list;
	id_mgr_init(&list);
	defer(id_mgr_free, &list);
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
		id_mgr_add(&list, id);
	}

	// write the list of partitions
	for (int i = 0; i < list.list_count; i++)
	{
		auto id = buf_u64(&list.list)[i];
		encode_array(buf);
		// path
		snprintf(path, sizeof(path), "checkpoints/%" PRIu64 "/%" PRIu64,
		         checkpoint, id);
		encode_cstr(buf, path);
		// size
		auto size = fs_size("%s/checkpoints/%" PRIu64 "/%" PRIu64, state_directory(),
		                    checkpoint, id);
		if (size == -1)
			error_system();
		encode_integer(buf, size);
		encode_array_end(buf);
	}

	// add catalog.json file last
	encode_array(buf);
	// path
	snprintf(path, sizeof(path), "checkpoints/%" PRIu64 "/catalog.json", checkpoint);
	encode_cstr(buf, path);
	// size
	auto size = fs_size("%s/checkpoints/%" PRIu64 "/catalog.json",
	                    state_directory(),
	                    checkpoint);
	if (size == -1)
		error_system();
	encode_integer(buf, size);
	encode_array_end(buf);
}

uint64_t
checkpoint_mgr_snapshot(CheckpointMgr* self)
{
	// create snapshot
	id_mgr_add(&self->list_snapshot, 0);

	// get the last checkpoint id
	return id_mgr_max(&self->list);
}
