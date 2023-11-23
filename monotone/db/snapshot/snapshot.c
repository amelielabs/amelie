
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_snapshot.h>

hot static void
snapshot_storage(SnapshotFile* file, SnapshotBatch* batch, Storage* storage)
{
	// MSG_SNAPSHOT_STORAGE
	snapshot_batch_add_storage(batch, &storage->config->id);

	Iterator* it = NULL;
	Exception e;
	if (try(&e))
	{
		auto primary = storage_primary(storage);
		auto def = &primary->config->def;

		it = index_read(primary);
		iterator_open(it, NULL);
		for (;;)
		{
			auto row = iterator_at(it);
			if (! row)
				break;

			// MSG_SNAPSHOT_ROW
			snapshot_batch_add(batch, def, row);

			// flush
			if (snapshot_batch_full(batch))
			{
				snapshot_file_write(file, &batch->iov);
				snapshot_batch_reset(batch);
			}

			iterator_next(it);
		}

		// flush
		if (snapshot_batch_has(batch))
			snapshot_file_write(file, &batch->iov);
	}

	if (it)
	{
		iterator_close(it);
		iterator_free(it);
	}

	if (catch(&e))
		rethrow();
}

pid_t
snapshot(StorageMgr* storage_mgr, Condition* on_complete, Uuid* id)
{
	// create new process
	auto pid = fork();
	switch (pid) {
	case -1:
		error_system();
	case  0:
		break;
	default:
		return pid;
	}

	// dump storages related to the shard uuid
	SnapshotFile file;
	snapshot_file_init(&file);

	SnapshotBatch batch;
	snapshot_batch_init(&batch);

	Exception e;
	if (try(&e))
	{
		// prepare snapshot writer batch
		snapshot_batch_create(&batch, 5000);

		// create snapshot file as uuid.lsn.incomplete
		snapshot_file_create(&file, id, config_lsn());

		// dump storage data related to the id
		list_foreach(&storage_mgr->list)
		{
			auto storage = list_at(Storage, link);
			if (! uuid_compare(&storage->config->id_shard, id))
				continue;
			snapshot_storage(&file, &batch, storage);
			snapshot_batch_reset(&batch);
		}

		// sync and rename
		snapshot_file_complete(&file);
	}

	bool error = false;
	if (catch(&e))
	{
		error = true;

		// todo: can throw
		snapshot_file_abort(&file);
	}

	snapshot_batch_free(&batch);

	// signal waiter process
	condition_signal(on_complete);

	// done
	_exit(error ? EXIT_FAILURE : EXIT_SUCCESS);
	return -1;
}
