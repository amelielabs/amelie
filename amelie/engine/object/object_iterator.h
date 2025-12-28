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

typedef struct ObjectIterator ObjectIterator;

struct ObjectIterator
{
	Iterator       it;
	RegionIterator region_iterator;
	SpanIterator   span_iterator;
	SpanRegion*    current;
	Object*        object;
	Keys*          keys;
	Reader         reader;
};

hot static inline bool
object_iterator_open_region(ObjectIterator* self, Row* row)
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
object_iterator_open(ObjectIterator* self, Keys* keys, Object* object, Row* row)
{
	self->object  = object;
	self->keys    = keys;
	self->current = NULL;

	region_iterator_init(&self->region_iterator);
	span_iterator_init(&self->span_iterator);
	span_iterator_open(&self->span_iterator, &object->index, &object->index_data,
	                    keys, row);

	self->current = span_iterator_at(&self->span_iterator);
	if (self->current == NULL)
		return false;

	reader_open(&self->reader, object);
	return object_iterator_open_region(self, row);
}

hot static inline bool
object_iterator_has(ObjectIterator* self)
{
	return region_iterator_has(&self->region_iterator);
}

hot static inline Row*
object_iterator_at(ObjectIterator* self)
{
	return region_iterator_at(&self->region_iterator);
}

hot static inline void
object_iterator_next(ObjectIterator* self)
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

		object_iterator_open_region(self, NULL);
	}
}

static inline void
object_iterator_free(ObjectIterator* self)
{
	reader_reset(&self->reader);
	reader_free(&self->reader);
}

static inline void
object_iterator_reset(ObjectIterator* self)
{
	reader_reset(&self->reader);
	region_iterator_reset(&self->region_iterator);
}

static inline void
object_iterator_init(ObjectIterator* self)
{
	self->object  = NULL;
	self->current = NULL;
	reader_init(&self->reader);
	span_iterator_init(&self->span_iterator);
	region_iterator_init(&self->region_iterator);
	auto it = &self->it;
	it->has   = (IteratorHas)object_iterator_has;
	it->at    = (IteratorAt)object_iterator_at;
	it->next  = (IteratorNext)object_iterator_next;
	it->close = (IteratorClose)object_iterator_free;
}
