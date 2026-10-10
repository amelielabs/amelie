
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

hot static void
part_cleanup_index(PartCleanup* self)
{
	// delete all rows for this timeline
	auto part = self->part;
	auto heap = part->heap;

	Timeline timeline;
	timeline_init(&timeline);
	timeline_set_timeline(&timeline, self->timeline);

	auto primary = part_primary(part);
	auto it = index_iterator(primary);
	defer(iterator_close, it);
	iterator_open(it, &timeline, NULL);

	IndexOp op =
	{
		.row      = NULL,
		.row_prev = NULL,
		.it       = it,
		.delta    = 0
	};
	for (;; iterator_next(it))
	{
		auto row = iterator_at(it);
		if (! row)
			break;
		if (! row_visible(row, &timeline))
			continue;
		op.row = NULL;
		op.it  = it;
		index_delete(primary, &op);
		for (auto index = primary->next; index; index = index->next)
		{
			op.row = row;
			op.it  = NULL;
			index_delete(index, &op);
		}
		row_free(heap, &part->flats, row);
	}

	usage_update(self->part->arg->memory, op.delta);
}

hot void
part_cleanup_heap(PartCleanup* self)
{
	// free all heap rows for this timeline
	auto part     = self->part;
	auto timeline = self->timeline;
	auto heap     = part->heap;
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, false);
	for (;; heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (! row)
			break;
		if (row->timeline != timeline)
			continue;
		row_free(heap, &part->flats, row);
	}
}

hot void
part_cleanup(PartCleanup* self)
{
	if (part_primary(self->part))
		part_cleanup_index(self);
	else
		part_cleanup_heap(self);
}
