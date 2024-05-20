
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>

void
checkpoint_init(Checkpoint* self, uint64_t lsn)
{
	self->lsn                = lsn;
	self->pid                = -1;
	self->on_complete        = NULL;
	self->list_count         =  0;
	self->list_count_skipped =  0;
	list_init(&self->list);
}

void
checkpoint_free(Checkpoint* self)
{
	if (self->on_complete)
	{
		condition_free(self->on_complete);
		self->on_complete = NULL;
	}
}

static void
checkpoint_add_storage(Checkpoint* self, Storage* storage)
{
	auto part = part_tree_min(&storage->tree);
	for (; part; part = part_tree_next(&storage->tree, part))
	{
		// snapshot already created
		if (part->snapshot >= self->lsn)
		{
			self->list_count_skipped++;
			continue;
		}

		// snapshot created in the past without new updates
		//
		// commit lsn <= snapshot lsn <= lsn
		auto index = part_primary(part);
		auto lsn_commit = index_lsn(index);
		if (lsn_commit <= part->snapshot)
		{
			self->list_count_skipped++;
			continue;
		}

		// snapshot created in the past with new updates
		//
		// snapshot lsn < commit lsn <= lsn
		list_init(&part->link_cp);
		list_append(&self->list, &part->link_cp);
		self->list_count++;
	}
}

void
checkpoint_add(Checkpoint* self, StorageMgr* storage_mgr)
{
	list_foreach(&storage_mgr->list)
	{
		auto storage = list_at(Storage, link);
		checkpoint_add_storage(self, storage);
	}
}

hot static bool
checkpoint_main(Checkpoint* self)
{
	Snapshot snapshot;
	snapshot_init(&snapshot);

	Exception e;
	if (try(&e))
	{
		// create primary index snapshot per partition
		list_foreach(&self->list)
		{
			auto part = list_at(Part, link_cp);
			auto primary = part_primary(part);

			SnapshotId id;
			snapshot_id_set(&id, part->id_storage, part->min, part->max, self->lsn);
			snapshot_reset(&snapshot);
			snapshot_create(&snapshot, &id, primary);
		}
	}
	snapshot_free(&snapshot);

	bool error = false;
	if (catch(&e))
		error = true;

	return error;
}

void
checkpoint_run(Checkpoint* self)
{
	if (self->list_count == 0)
		return;

	self->on_complete = condition_create();
	log("checkpoint: %" PRIu64, self->lsn);

	// create new process
	self->pid = fork();
	switch (self->pid) {
	case -1:
		error_system();
	case  0:
		break;
	default:
		return;
	}

	// create snapshots
	auto error = checkpoint_main(self);

	// signal waiter process
	condition_signal(self->on_complete);

	// done
	_exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
}

void
checkpoint_wait(Checkpoint* self)
{
	assert(self->list_count > 0);

	condition_wait(self->on_complete, -1);
	condition_free(self->on_complete);
	self->on_complete = NULL;

	int status = 0;
	int rc = waitpid(self->pid, &status, 0);
	if (rc == -1)
		error_system();

	bool failed = false;
	if (WIFEXITED(status))
	{
		if (WEXITSTATUS(status) == EXIT_FAILURE)
			failed = true;
	} else {
		failed = true;
	}

	if (failed)
		error("failed to create snapshot");

	log("checkpoint: %d partitions created (%d skipped)", self->list_count,
	    self->list_count_skipped);
	log("checkpoint: %" PRIu64 " complete", self->lsn);

	// update snaphot lsn
	list_foreach(&self->list)
	{
		auto part = list_at(Part, link_cp);
		part->snapshot = self->lsn;
	}
}
