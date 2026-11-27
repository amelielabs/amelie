
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
#include <amelie_server>
#include <amelie_heap>
#include <amelie_transaction.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_service.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

void
recover_init(Recover* self, Storage* storage, bool panic)
{
	self->processed = 0;
	self->panic     = panic;
	self->storage   = storage;
}

void
recover_free(Recover* self)
{
	unused(self);
}

void
recover_wait(Recover* self)
{
	auto service = &self->storage->service;
	service_sync(service);

	// check for errors
	uint64_t errors = atomic_u64_of(&service->errors);
	if (unlikely(errors))
	{
		if (self->panic)
			panic("recover: error during playback");
		else
			error("recover: error during playback");
	}
}

void
recover_next(Recover* self, Record* record)
{
	auto service = &self->storage->service;
	auto db = catalog_find_db_by(&self->storage->catalog, &record->id, false);
	if (! db)
		return;

	// global (catalog)
	if ((record->flags & RECORD_UTILITY) > 0)
	{
		// applied
		if (likely(self->storage->catalog_lsn >= record->lsn))
			return;

		// sync
		recover_wait(self);

		// execute (under amelie)
		service_add(service, ACTION_RECORD, db->rel.id, NULL,
		            record->size,
		            record);

		// sync
		recover_wait(self);
		return;
	}

	// local (per database)

	// applied (as part of the last checkpoint)
	if (db->checkpoints.current &&
	    db->checkpoints.current->id >= record->lsn)
		return;

	// execute
	service_add(service, ACTION_RECORD, db->rel.id, NULL,
	            record->size,
	            record);

	// sync
	self->processed++;
	if ((self->processed % 100000) == 0)
		recover_wait(self);
}

static void
recover_wal_main(Recover* self)
{
	auto wal = &self->storage->wal;
	auto wal_crc = config()->wal_crc;

	uint64_t id = 0;
	for (;;)
	{
		uint64_t count = 0;
		uint64_t size  = 0;

		WalCursor cursor;
		wal_cursor_init(&cursor);
		defer(wal_cursor_close, &cursor);
		wal_cursor_open(&cursor, wal, id, false, wal_crc, false);
		for (;;)
		{
			auto buf = wal_cursor_next(&cursor);
			if (! buf)
				break;
			defer_buf(buf);

			auto record = (Record*)buf->start;
			size += record->size;
			self->processed++;
			count++;

			// set lsn
			assert(state_lsn() < record->lsn);
			state_lsn_set(record->lsn);

			// execute
			recover_next(self, (Record*)buf->start);
		}
		if (! wal_cursor_active(&cursor))
			break;

		// sync
		recover_wait(self);

		// report
		clock_update(&am_task->clock);
		info("recover: wal/{u64} ({.2f} MiB, {u64} transactions)", cursor.file->id,
		     (double)size / 1024 / 1024, count);

		auto next = wal_find(wal, cursor.file->id, true);
		if (! next)
			break;

		id = next->id;
		wal_file_unpin(next);
	}
}

void
recover_wal(Recover* self)
{
	if (error_catch( recover_wal_main(self)) )
	{
		info("recover: wal replay error, last valid lsn is: {u64}",
		     state_lsn());
		rethrow();
	}
}
