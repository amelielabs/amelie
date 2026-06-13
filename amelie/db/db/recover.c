
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
#include <amelie_db.h>

void
recover_init(Recover* self, Db* db, RecoverIf* iface, void* iface_arg)
{
	self->iface     = iface;
	self->iface_arg = iface_arg;
	self->state     = NULL;
	self->db        = db;
}

void
recover_free(Recover* self)
{
	if (self->state)
		self->iface->free(self);
	unused(self);
}

static void
recover_wal_main(Recover* self)
{
	// open wal files and maybe truncate wal files according
	// to the wal_truncate option
	auto wal = &self->db->wal;
	wal_open(wal);

	// prepare recovery state
	self->iface->create(self);

	// replay wals starting after last checkpoint
	uint64_t id = state_checkpoint() + 1;
	for (;;)
	{
		uint64_t count = 0;
		uint64_t size  = 0;

		WalCursor cursor;
		wal_cursor_init(&cursor);
		defer(wal_cursor_close, &cursor);

		auto wal_crc = opt_int_of(&config()->wal_crc);
		wal_cursor_open(&cursor, wal, id, false, wal_crc, false);
		for (;;)
		{
			if (! wal_cursor_next(&cursor))
				break;

			// validate crc
			auto record = wal_cursor_at(&cursor);
			if (opt_int_of(&config()->wal_crc))
				if (unlikely(! record_validate(record)))
					error("recover: record crc mismatch");

			// execute
			self->iface->execute(self, record);

			size += record->size;
			count++;

		}
		if (! wal_cursor_active(&cursor))
			break;

		info("recover: wal/{u64} ({.2f} MiB, {u64} rows)", cursor.file->id,
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
