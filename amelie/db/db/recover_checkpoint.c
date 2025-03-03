
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
#include <amelie_wal.h>
#include <amelie_db.h>

hot static void
recover_partition(Part* self)
{
	auto checkpoint = state_checkpoint();

	SnapshotCursor cursor;
	snapshot_cursor_init(&cursor);
	defer(snapshot_cursor_close, &cursor);

	snapshot_cursor_open(&cursor, checkpoint, self->config->id);
	uint64_t count = 0;
	for (;;)
	{
		auto buf = snapshot_cursor_next(&cursor);
		if (! buf)
			break;
		defer_buf(buf);
		auto pos = msg_of(buf)->data;
		auto row = row_copy(&self->heap, (Row*)pos);
		part_ingest(self, row);
		count++;
	}

	double size = (double)cursor.file.size / 1024 / 1024;
	info("checkpoints/%" PRIu64 "/%" PRIu64 " (%.2f MiB, %" PRIu64 " rows)",
	     checkpoint, self->config->id, size, count);
}

hot void
recover_checkpoint(Db* self, Uuid* id)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			if (! uuid_compare(&part->config->backend, id))
				recover_partition(part);
		}
	}
}
