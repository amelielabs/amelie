
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
snapshot_mgr_init(SnapshotMgr* self, uint64_t storage)
{
	self->storage = storage;
	id_mgr_init(&self->list);
	id_mgr_init(&self->list_snapshot);
}

void
snapshot_mgr_free(SnapshotMgr* self)
{
	id_mgr_free(&self->list);
	id_mgr_free(&self->list_snapshot);
}

void
snapshot_mgr_gc(SnapshotMgr* self)
{
    uint64_t min = id_mgr_min(&self->list_snapshot);

	// remove snapshot files < min
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
			snprintf(path, sizeof(path), "%s/%" PRIu64 ".%020" PRIu64,
			         config_directory(),
			         self->storage,
			         ids[i]);
			log("gc: %s", path);
			fs_unlink("%s", path);
		}
	}
}

void
snapshot_mgr_add(SnapshotMgr* self, uint64_t id)
{
	id_mgr_add(&self->list, id);
}
