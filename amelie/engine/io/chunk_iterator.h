#pragma once

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

typedef struct ChunkIterator ChunkIterator;

struct ChunkIterator
{
	Iterator       it;
	RegionIterator region_iterator;
	SpanIterator   span_iterator;
	SpanRegion*    current;
	Chunk*         chunk;
	Keys*          keys;
	Reader         reader;
};

hot static inline bool
chunk_iterator_open_region(ChunkIterator* self, Row* row)
{
	// read region from file
	auto region = reader_execute(&self->reader, self->current);

	// prepare region iterator
	region_iterator_reset(&self->region_iterator);
	return region_iterator_open(&self->region_iterator, region,
	                             self->keys,
	                             row);
}

hot static inline bool
chunk_iterator_open(ChunkIterator* self, Keys* keys, Chunk* chunk, Row* row)
{
	self->chunk   = chunk;
	self->keys    = keys;
	self->current = NULL;

	region_iterator_init(&self->region_iterator);
	span_iterator_init(&self->span_iterator);
	span_iterator_open(&self->span_iterator, &chunk->span, &chunk->span_data,
	                    keys, row);

	self->current = span_iterator_at(&self->span_iterator);
	if (self->current == NULL)
		return false;

	reader_open(&self->reader, chunk);
	return chunk_iterator_open_region(self, row);
}

hot static inline bool
chunk_iterator_has(ChunkIterator* self)
{
	return region_iterator_has(&self->region_iterator);
}

hot static inline Row*
chunk_iterator_at(ChunkIterator* self)
{
	return region_iterator_at(&self->region_iterator);
}

hot static inline void
chunk_iterator_next(ChunkIterator* self)
{
	if (unlikely(self->current == NULL))
		return;

	// iterate over current region
	region_iterator_next(&self->region_iterator);

	for (;;)
	{
		if (likely(region_iterator_has(&self->region_iterator)))
			break;

		// read next region
		span_iterator_next(&self->span_iterator);
		self->current = span_iterator_at(&self->span_iterator);
		if (unlikely(self->current == NULL))
			break;

		chunk_iterator_open_region(self, NULL);
	}
}

static inline void
chunk_iterator_free(ChunkIterator* self)
{
	reader_reset(&self->reader);
	reader_free(&self->reader);
}

static inline void
chunk_iterator_reset(ChunkIterator* self)
{
	reader_reset(&self->reader);
	region_iterator_reset(&self->region_iterator);
}

static inline void
chunk_iterator_init(ChunkIterator* self)
{
	self->chunk   = NULL;
	self->current = NULL;
	reader_init(&self->reader);
	span_iterator_init(&self->span_iterator);
	region_iterator_init(&self->region_iterator);
	auto it = &self->it;
	it->has   = (IteratorHas)chunk_iterator_has;
	it->at    = (IteratorAt)chunk_iterator_at;
	it->next  = (IteratorNext)chunk_iterator_next;
	it->close = (IteratorClose)chunk_iterator_free;
}
