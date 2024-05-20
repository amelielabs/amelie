
//
// sonata.
//
// SQL Database for JSON.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_storage.h>

void
snapshot_mgr_init(SnapshotMgr* self)
{
	uuid_init(&self->uuid);
	id_mgr_init(&self->list);
	id_mgr_init(&self->list_snapshot);
}

void
snapshot_mgr_free(SnapshotMgr* self)
{
	id_mgr_free(&self->list);
	id_mgr_free(&self->list_snapshot);
}

static inline int64_t
snapshot_file_id_of(const char* path, bool* incomplete)
{
	uint64_t id = 0;
	while (*path && *path != '.')
	{
		if (unlikely(! isdigit(*path)))
			return -1;
		id = (id * 10) + *path - '0';
		path++;
	}
	if (*path == '.')
	{
		if (! strcmp(path, ".incomplete"))
			*incomplete = true;
		return -1;
	}
	*incomplete = false;
	return id;
}

static void
closedir_guard(DIR* self)
{
	closedir(self);
}

static void
snapshot_mgr_recover(SnapshotMgr* self, char* path)
{
	// open and read log directory
	DIR* dir = opendir(path);
	if (unlikely(dir == NULL))
		error("snapshot: directory '%s' open error", path);
	guard(closedir_guard, dir);
	for (;;)
	{
		auto entry = readdir(dir);
		if (entry == NULL)
			break;
		if (entry->d_name[0] == '.')
			continue;
		bool incomplete;
		int64_t id = snapshot_file_id_of(entry->d_name, &incomplete);
		if (unlikely(id == -1))
			continue;
		if (incomplete)
		{
			log("snapshot: removing incomplete snapshot '%s/%s'", path, entry->d_name);
			fs_unlink("%s/%s", path, entry->d_name);
			continue;
		}
		snapshot_mgr_add(self, id);
	}
}

void
snapshot_mgr_open(SnapshotMgr* self, Uuid* uuid)
{
	self->uuid = *uuid;

	char uuid_sz[UUID_SZ];
	uuid_to_string(uuid, uuid_sz, sizeof(uuid_sz));

	// create <base>/storage_uuid/, if not exists
	char path[PATH_MAX];
	snprintf(path, PATH_MAX, "%s/%s", config_directory(), uuid_sz);
	if (! fs_exists("%s", path))
		fs_mkdir(0755, "%s", path);

	// read file list
	snapshot_mgr_recover(self, path);
}

void
snapshot_mgr_gc(SnapshotMgr* self)
{
    uint64_t min = id_mgr_min(&self->list_snapshot);

	// remove snapshot files < min
	char uuid_sz[UUID_SZ];
	uuid_to_string(&self->uuid, uuid_sz, sizeof(uuid_sz));

	Buf list;
	buf_init(&list);
	guard(buf_free, &list);

	int list_count;
	list_count = id_mgr_gc_between(&self->list, &list, min);

	// remove files by id
	if (list_count > 0)
	{
		char path[PATH_MAX];
		uint64_t *ids = (uint64_t*)list.start;
		int i = 0;
		for (; i < list_count; i++)
		{
			snprintf(path, sizeof(path), "%s/%s/%020" PRIu64,
			         config_directory(),
			         uuid_sz,
			         ids[i]);
			log("gc %" PRIu64 ": %s", ids[i], uuid_sz);
			fs_unlink("%s", path);
		}
	}
}

void
snapshot_mgr_add(SnapshotMgr* self, uint64_t id)
{
	id_mgr_add(&self->list, id);
}
