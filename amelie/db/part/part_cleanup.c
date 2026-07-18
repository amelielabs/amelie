
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
#include <amelie_cdc.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_part.h>

hot static void
part_cleanup_main_head(PartCleanup* self, Heap* heap, Row* row)
{
	// row is a main head
	//
	// [head] <- [row] <- ...
	//
	auto primary = part_primary(self->part);
	if (row->deleted)
	{
		// remove whole chain from the indexes
		index_delete_by(primary, row);
		for (auto index = primary->next; index; index = index->next)
			index_delete_by(index, row);
		row->head = false;
	} else
	{
		// keep the head (free the rest)
		auto head = row;
		row = row_prev(row, heap);
		row_prev_set(head, NULL);
	}

	while (row)
	{
		auto prev = row_prev(row, heap);
		row_free(heap, &self->part->flats, row);
		row = prev;
	}
}

hot static void
part_cleanup_main_prev(PartCleanup* self, Heap* heap, Row* row)
{
	// row is a head (but not main)
	//
	// [row] <- [head] <- ...

	// match the last main row
	auto head      = row;
	auto head_next = row;
	for (; head_next; head_next = row_prev(head_next, heap))
		if (head_next->main)
			break;
	head->head = false;

	// drop whole chain, if the main row is delete
	if (head_next && head_next->deleted)
		head_next = NULL;

	// delete or replace the index
	auto primary = part_primary(self->part);
	if (head_next)
	{
		index_replace_by(primary, head_next);
		for (auto index = primary->next; index; index = index->next)
			index_replace_by(index, head_next);

		// mark new head
		head_next->head = true;

		// free all other rows except current new head
		while (row)
		{
			auto prev = row_prev(row, heap);
			if (row != head_next)
				row_free(heap, &self->part->flats, row);
			row = prev;
		}

		row_prev_set(head_next, NULL);
		return;
	}

	// free whole chain
	index_delete_by(primary, row);
	for (auto index = primary->next; index; index = index->next)
		index_delete_by(index, row);
	while (row)
	{
		auto prev = row_prev(row, heap);
		row_free(heap, &self->part->flats, row);
		row = prev;
	}
}

hot static void
part_cleanup_main(PartCleanup* self)
{
	auto part = self->part;
	auto heap = part->heap;
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, NULL);

	// deep cleaning after last clone drop
	for (;; heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (! row)
			break;

		if (! row->head)
			continue;

		if (row->main)
			part_cleanup_main_head(self, heap, row);
		else
			part_cleanup_main_prev(self, heap, row);
	}
}

hot static void
part_cleanup_clone(PartCleanup* self)
{
	// cleanup clone (timeline) related rows after drop
	auto part     = self->part;
	auto timeline = self->timeline;
	auto heap     = part->heap;
	HeapIterator it;
	heap_iterator_init(&it);
	heap_iterator_open(&it, heap, NULL);

	auto primary = part_primary(part);
	for (;; heap_iterator_next(&it))
	{
		auto row = heap_iterator_at(&it);
		if (! row)
			break;

		if (! row->head)
			continue;

		// update indexes
		if (row->timeline == timeline)
		{
			// get a first row not related to the timeline
			auto head      = row;
			auto head_next = row;
			for (; head_next; head_next = row_prev(head_next, heap))
				if (head_next->timeline != timeline)
					break;

			// delete or replace the index
			head->head = false;
			if (head_next)
			{
				index_replace_by(primary, head_next);
				for (auto index = primary->next; index; index = index->next)
					index_replace_by(index, head_next);

				// mark new head
				head_next->head = true;
			} else
			{
				index_delete_by(primary, head);
				for (auto index = primary->next; index; index = index->next)
					index_delete_by(index, head);
			}
		}

		// free all rows related to the timeline
		auto parent = NULL;
		while (row)
		{
			auto prev = row_prev(row, heap);
			if (row->timeline == timeline)
			{
				if (parent)
					row_prev_set(parent, prev);
				row_free(heap, &part->flats, row);
			} else {
				parent = row;
			}
			row = prev;
		}
	}
}

hot void
part_cleanup_run(PartCleanup* self)
{
	// cleanup partition after last clone drop
	if (self->part->arg->timelines->list_count == 0)
		return part_cleanup_main(self);

	// partition still has clones, free rows only related
	// to the timelime
	return part_cleanup_clone(self);
}
