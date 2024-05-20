
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
checkpoint_init(Checkpoint* self)
{
	self->workers       = NULL;
	self->workers_count = 0;
	self->rr            = 0;
}

void
checkpoint_free(Checkpoint* self)
{
	if (self->workers)
	{
		for (int i = 0; i < self->workers_count; i++)
		{
			auto worker = &self->workers[i];
			if (worker->on_complete)
				condition_free(worker->on_complete);
		}
		so_free(self->workers);
	}
	self->workers = NULL;
	self->workers_count = 0;
}

void
checkpoint_prepare(Checkpoint* self, int count)
{
	self->workers_count = count;
	self->workers = so_malloc(sizeof(CheckpointWorker) * count);
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		worker->pid         = -1;
		worker->on_complete = condition_create();
		worker->list_count  = 0;
		list_init(&worker->list);
	}
}

static void
checkpoint_add_storage(Checkpoint* self, Storage* storage)
{
	// ensure index has updates since the last snapshot
	auto index = storage_primary(storage);

	// last snapshot lsn < commit lsn
	uint64_t max = snapshot_mgr_last(&storage->snapshot_mgr);
	if (max == index->lsn)
		return;

	// get next worker
	if (self->rr == self->workers_count)
		self->rr = 0;
	int order = self->rr++;
	auto worker = &self->workers[order];

	list_init(&storage->link_cp);
	list_append(&worker->list, &storage->link_cp);
	worker->list_count++;
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
checkpoint_worker_main(CheckpointWorker* self)
{
	Snapshot snapshot;
	snapshot_init(&snapshot);

	Exception e;
	if (enter(&e))
	{
		// create primary index snapshot per storage
		list_foreach(&self->list)
		{
			auto storage = list_at(Storage, link_cp);
			auto primary = storage_primary(storage);

			SnapshotId id;
			snapshot_id_set(&id, &storage->config->id, primary->lsn);
			snapshot_reset(&snapshot);
			snapshot_create(&snapshot, &id, primary);
		}
	}
	snapshot_free(&snapshot);

	if (leave(&e))
	{ }

	return e.triggered;
}

static void
checkpoint_worker_run(CheckpointWorker* self)
{
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
	auto error = checkpoint_worker_main(self);

	// signal waiter process
	condition_signal(self->on_complete);

	// done
	_exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
}

static bool
checkpoint_worker_wait(CheckpointWorker* self)
{
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

	return failed;
}

void
checkpoint_run(Checkpoint* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;
		checkpoint_worker_run(worker);
	}
}

void
checkpoint_wait(Checkpoint* self)
{
	int errors = 0;
	for (int i = 0; i < self->workers_count; i++)
	{
		auto worker = &self->workers[i];
		if (worker->list_count == 0)
			continue;

		bool error;
		error = checkpoint_worker_wait(worker);
		if (error)
		{
			errors++;
			continue;
		}

		// add snapshots update snaphot lsn
		list_foreach(&worker->list)
		{
			auto storage = list_at(Storage, link_cp);
			auto primary = storage_primary(storage);
			snapshot_mgr_add(&storage->snapshot_mgr, primary->lsn);
		}
	}

	if (errors > 0)
		error("failed to create snapshot");
}
