
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko.
// AGPL-3.0 Licensed.
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
#include <amelie_wal.h>
#include <amelie_db.h>

hot static void
recover_partition(Part* self)
{
	auto checkpoint = config_checkpoint();

	SnapshotCursor cursor;
	snapshot_cursor_init(&cursor);
	guard(snapshot_cursor_close, &cursor);

	snapshot_cursor_open(&cursor, checkpoint, self->config->id);
	uint64_t count = 0;
	for (;;)
	{
		auto buf = snapshot_cursor_next(&cursor);
		if (! buf)
			break;
		guard_buf(buf);
		auto pos = msg_of(buf)->data;
		part_ingest(self, &pos);
		count++;
	}

	double size = cursor.file.size / 1024 / 1024;
	info("recover: %" PRIu64 "/%" PRIu64 ".part (%.2f MiB, %" PRIu64 " rows)",
	     checkpoint, self->config->id, size, count);
}

hot void
recover_checkpoint(Db* self, Uuid* node)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Handle, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			if (! uuid_compare(&part->config->node, node))
				continue;
			recover_partition(part);
		}
	}
}
