
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

hot void
part_cleanup(PartCleanup* self)
{
	// remove all rows related to the timeline
	auto part     = self->part;
	auto timeline = self->timeline;
	auto heap     = part->heap;
	auto primary  = part_primary(part);

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

		// update indexes
		if (primary)
		{
			IndexOp op;
			index_op_set(&op, row);
			index_delete(primary, &op);
			for (auto index = primary->next; index; index = index->next)
				index_delete(index, &op);
			usage_update(part->arg->memory, op.delta);
		}

		// free
		row_free(heap, &part->flats, row);
	}
}
