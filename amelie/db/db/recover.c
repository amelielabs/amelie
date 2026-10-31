
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
#include <amelie_type.h>
#include <amelie_storage.h>
#include <amelie_flat.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_db.h>

void
recover_init(Recover* self, Db* db)
{
	self->db = db;
	recover_sync_init(&self->sync);
}

void
recover_free(Recover* self)
{
	unused(self);
}

void
recover_wait(Recover* self)
{
	recover_sync(&self->sync);
}

static inline User*
recover_read(Recover* self, RecordMsg* msg)
{
	Str name;
	str_init(&name);

	auto record = msg->record;
	auto pos = record_data(record);
	unpack_array(&pos);
	while (! unpack_array_end(&pos))
	{
		int64_t field;
		unpack_int(&pos, &field);

		// META_USER
		if (field == 0)
		{
			unpack_str(&pos, &name);
			break;
		}
		data_skip(&pos);
	}

	if (str_empty(&name))
		error("recover: user field not found");

	return catalog_find_user(&self->db->catalog, &name, true);
}

void
recover_next(Recover* self, RecordMsg* msg)
{
	auto user = recover_read(self, msg);

	// sync on ddl and multi-user transactions (wait for completion)
	auto sync = &self->sync;
	auto sync_after = false;
	if (unlikely((msg->record->flags & RECORD_UTILITY) > 0 ||
	             (msg->record->flags & RECORD_MULTI_USER) > 0))
	{
		recover_sync(sync);
		sync_after = true;
	}

	// send for execution
	recover_sync_add(sync);
	msg->user = user;
	msg->arg  = sync;
	task_send(user->track.compute, &msg->msg);

	// sync
	uint64_t total = atomic_u64_of(&sync->total);
	if (sync_after || !(total % 10000))
		recover_sync(sync);
}

static void
recover_wal_main(Recover* self)
{
	auto wal = &self->db->wal;
	auto wal_crc = opt_int_of(&config()->wal_crc);

	// replay wals starting after last checkpoint
	uint64_t id = state_checkpoint() + 1;
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
			auto msg = wal_cursor_next_msg(&cursor);
			if (! msg)
				break;
			size += msg->record->size;
			count++;

			// execute
			recover_next(self, msg);
		}
		if (! wal_cursor_active(&cursor))
			break;

		// sync
		recover_wait(self);

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
