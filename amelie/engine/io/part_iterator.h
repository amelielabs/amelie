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

typedef struct PartIterator PartIterator;

struct PartIterator
{
	Iterator       it;
	RegionIterator region_iterator;
	SpanIterator   span_iterator;
	SpanRegion*    current;
	Part*          part;
	Keys*          keys;
	Reader         reader;
};

hot static inline bool
part_iterator_open_region(PartIterator* self, Row* row)
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
part_iterator_open(PartIterator* self, Part* part, Row* row)
{
	self->part    = part;
	self->keys    = &part_primary(part)->config->keys;
	self->current = NULL;

	region_iterator_init(&self->region_iterator);
	span_iterator_init(&self->span_iterator);
	span_iterator_open(&self->span_iterator, &part->span, &part->span_data,
	                    self->keys, row);

	self->current = span_iterator_at(&self->span_iterator);
	if (self->current == NULL)
		return false;

	reader_open(&self->reader, part);
	return part_iterator_open_region(self, row);
}

hot static inline bool
part_iterator_has(PartIterator* self)
{
	return region_iterator_has(&self->region_iterator);
}

hot static inline Row*
part_iterator_at(PartIterator* self)
{
	return region_iterator_at(&self->region_iterator);
}

hot static inline void
part_iterator_next(PartIterator* self)
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

		part_iterator_open_region(self, NULL);
	}
}

static inline void
part_iterator_free(PartIterator* self)
{
	reader_reset(&self->reader);
	reader_free(&self->reader);
}

static inline void
part_iterator_reset(PartIterator* self)
{
	reader_reset(&self->reader);
	region_iterator_reset(&self->region_iterator);
}

static inline void
part_iterator_init(PartIterator* self)
{
	self->part    = NULL;
	self->current = NULL;
	reader_init(&self->reader);
	span_iterator_init(&self->span_iterator);
	region_iterator_init(&self->region_iterator);
	auto it = &self->it;
	it->has   = (IteratorHas)part_iterator_has;
	it->at    = (IteratorAt)part_iterator_at;
	it->next  = (IteratorNext)part_iterator_next;
	it->close = (IteratorClose)part_iterator_free;
}
