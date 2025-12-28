
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
#include <amelie_io.h>

void
merger_init(Merger* self)
{
	self->chunk = NULL;
	chunk_iterator_init(&self->chunk_iterator);
	heap_iterator_init(&self->heap_iterator);
	merge_iterator_init(&self->merge_iterator);
	writer_init(&self->writer);
}

void
merger_free(Merger* self)
{
	merger_reset(self);
	chunk_iterator_free(&self->chunk_iterator);
	merge_iterator_free(&self->merge_iterator);
	writer_free(&self->writer);
}

void
merger_reset(Merger* self)
{
	if (self->chunk)
	{
		chunk_free(self->chunk);
		self->chunk = NULL;
	}
	chunk_iterator_reset(&self->chunk_iterator);
	heap_iterator_reset(&self->heap_iterator);
	merge_iterator_reset(&self->merge_iterator);
	writer_reset(&self->writer);
}

hot static inline void
merger_write(Merger* self, MergerReq* req)
{
	auto writer = &self->writer;
	auto it     = &self->merge_iterator;
	auto chunk  = self->chunk;
	auto origin = req->origin;
	auto heap   = req->heap;

	writer_start(writer, req->source, &chunk->file);
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
		refreshes += origin->span.refreshes;
		if (origin->span.lsn > lsn)
			lsn = origin->span.lsn;
	}

	auto id = &chunk->id;
	writer_stop(writer, id, refreshes, 0, 0, lsn,
	            req->source->sync);

	// copy index
	span_writer_copy(&self->writer.span_writer,
	                 &chunk->span,
	                 &chunk->span_data);
}

hot void
merger_execute(Merger* self, MergerReq* req)
{
	auto origin = req->origin;

	// prepare heap iterator
	heap_iterator_open(&self->heap_iterator, req->heap, NULL);

	// prepare chunk iterator
	chunk_iterator_reset(&self->chunk_iterator);
	chunk_iterator_open(&self->chunk_iterator, req->keys, origin, NULL);

	// prepare merge iterator
	auto it = &self->merge_iterator;
	merge_iterator_reset(it);
	merge_iterator_add(it, &self->heap_iterator.it);
	merge_iterator_add(it, &self->chunk_iterator.it);
	merge_iterator_open(it, req->keys);

	// allocate and create incomplete chunkition file
	Id id = origin->id;
	self->chunk = chunk_allocate(req->source, &id);
	chunk_create(self->chunk, ID_INCOMPLETE);
	chunk_set(self->chunk, ID_INCOMPLETE);

	// write file
	merger_write(self, req);
}
