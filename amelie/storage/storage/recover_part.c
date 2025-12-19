
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
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

hot void
recover_part(Part* self)
{
	auto checkpoint = state_checkpoint();

	// <base>/checkpoints/<lsn>/<partition>
	char path[PATH_MAX];
	sfmt(path, sizeof(path),
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
	info(" %" PRIu64"/%05" PRIu64 " (%.2f MiB, %" PRIu64 " rows)",
	     checkpoint,
	     self->config->id, total, count);
}
