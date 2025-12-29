
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
#include <amelie_object.h>

void
merger_init(Merger* self)
{
	self->object = NULL;
	object_iterator_init(&self->object_iterator);
	heap_iterator_init(&self->heap_iterator);
	merge_iterator_init(&self->merge_iterator);
	writer_init(&self->writer);
}

void
merger_free(Merger* self)
{
	merger_reset(self);
	object_iterator_free(&self->object_iterator);
	merge_iterator_free(&self->merge_iterator);
	writer_free(&self->writer);
}

void
merger_reset(Merger* self)
{
	if (self->object)
	{
		object_free(self->object);
		self->object = NULL;
	}
	object_iterator_reset(&self->object_iterator);
	heap_iterator_reset(&self->heap_iterator);
	merge_iterator_reset(&self->merge_iterator);
	writer_reset(&self->writer);
}

hot static inline void
merger_write(Merger* self, MergerReq* req)
{
	auto writer = &self->writer;
	auto it     = &self->merge_iterator;
	auto object = self->object;
	auto origin = req->origin;
	auto heap   = req->heap;

	writer_start(writer, req->source, &object->file);
	for (;;)
	{
		auto row = merge_iterator_at(it);
		if (unlikely(row == NULL))
			break;
		if (row->is_delete)
		{
			merge_iterator_next(it);
			continue;
		}
		writer_add(writer, row);
		merge_iterator_next(it);
	}

	uint64_t refreshes = 1;
	uint64_t lsn = heap->lsn_max;
	if (origin->state != ID_NONE)
	{
		refreshes += origin->meta.refreshes;
		if (origin->meta.lsn > lsn)
			lsn = origin->meta.lsn;
	}

	auto id = &object->id;
	writer_stop(writer, id, refreshes, 0, 0, lsn,
	            req->source->sync);

	// copy and set meta data
	meta_writer_copy(&self->writer.meta_writer,
	                 &object->meta,
	                 &object->meta_data);
}

hot void
merger_execute(Merger* self, MergerReq* req)
{
	auto origin = req->origin;

	// prepare heap iterator
	heap_iterator_open(&self->heap_iterator, req->heap, NULL);

	// prepare object iterator
	object_iterator_reset(&self->object_iterator);
	object_iterator_open(&self->object_iterator, req->keys, origin, NULL);

	// prepare merge iterator
	auto it = &self->merge_iterator;
	merge_iterator_reset(it);
	merge_iterator_add(it, &self->heap_iterator.it);
	merge_iterator_add(it, &self->object_iterator.it);
	merge_iterator_open(it, req->keys);

	// allocate and create incomplete object file
	Id id = origin->id;
	self->object = object_allocate(req->source, &id);
	object_create(self->object, ID_INCOMPLETE);
	object_set(self->object, ID_INCOMPLETE);

	// write file
	merger_write(self, req);
}
