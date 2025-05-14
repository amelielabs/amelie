
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

	// <base>/checkpoints/<lsn>/<partition>
	char path[PATH_MAX];
	snprintf(path, sizeof(path),
	         "%s/checkpoints/%" PRIu64 "/%" PRIu64,
	         state_directory(),
	         checkpoint,
	         self->config->id);

	// read heap file
	auto size = heap_file_read(&self->heap, path);

	// rebuild indexes
	HeapCursor cursor;
	heap_cursor_init(&cursor);
	heap_cursor_open(&cursor, &self->heap);
	uint64_t count = 0;
	while (heap_cursor_has(&cursor))
	{
		auto row = (Row*)heap_cursor_at(&cursor)->data;
		part_ingest(self, row);
		count++;
		heap_cursor_next(&cursor);
	}

	double total = (double)size / 1024 / 1024;
	info("checkpoints/%" PRIu64 "/%" PRIu64 " (%.2f MiB, %" PRIu64 " rows)",
	     checkpoint, self->config->id, total, count);
}

hot void
recover_checkpoint(Db* self, Core* core)
{
	list_foreach(&self->table_mgr.mgr.list)
	{
		auto table = table_of(list_at(Relation, link));
		list_foreach(&table->part_list.list)
		{
			auto part = list_at(Part, link);
			if (part->core == core)
				recover_partition(part);
		}
	}
}
