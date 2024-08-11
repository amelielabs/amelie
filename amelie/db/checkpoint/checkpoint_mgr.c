
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
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
	uint64_t lsn = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		lsn = (lsn * 10) + *name - '0';
		name++;
	}
	if (*name == '.')
	{
		if (! strcmp(name, ".incomplete"))
			*incomplete = true;
		return -1;
	}
	*incomplete = false;
	return lsn;
}

static void
checkpoint_mgr_open_dir(CheckpointMgr* self)
{
	// read directory, get a list of checkpoints
	auto path = config_directory();
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("checkpoint: directory '%s' open error", path);
	guard(fs_opendir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		bool    incomplete;
		int64_t lsn;
		lsn = checkpoint_id_of(entry->d_name, &incomplete);
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
		config_lsn_follow(lsn);
	}

	// set last checkpoint
	auto last = id_mgr_max(&self->list);
	if (last != UINT64_MAX)
		var_int_set(&config()->checkpoint, last);
}

static void
checkpoint_mgr_open_catalog(CheckpointMgr* self)
{
	auto checkpoint = config_checkpoint();
	if (! checkpoint)
		return;

	// read and parse catalog content
	Buf buf;
	buf_init(&buf);
	guard(buf_free, &buf);
	file_import(&buf, "%s/%" PRIu64 "/catalog", config_directory(),
	            checkpoint);

	Str catalog_data;
	str_set_u8(&catalog_data, buf.start, buf_size(&buf));

	Json json;
	json_init(&json);
	guard(json_free, &json);
	json_parse(&json, global()->timezone, &catalog_data, NULL);

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
	uint64_t min = config_checkpoint();
	uint64_t snapshot_min = id_mgr_min(&self->list_snapshot);
	if (snapshot_min < min)
		min = snapshot_min;

	// remove checkpoints < min
	Buf list;
	buf_init(&list);
	guard(buf_free, &list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);
	if (list_count > 0)
	{
		auto lsns = buf_u64(&list);
		for (int i = 0; i < list_count; i++)
			fs_rmdir("%s/%" PRIu64, config_directory(), lsns[i]);
	}
}

void
checkpoint_mgr_add(CheckpointMgr* self, uint64_t id)
{
	id_mgr_add(&self->list, id);
}
