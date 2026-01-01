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

typedef struct MetaIterator MetaIterator;

struct MetaIterator
{
	Meta*       meta;
	Buf*        meta_data;
	MetaRegion* current;
	int         pos;
	Keys*       keys;
};

static inline void
meta_iterator_init(MetaIterator* self)
{
	self->meta      = NULL;
	self->meta_data = NULL;
	self->current   = NULL;
	self->pos       = 0;
	self->keys      = NULL;
}

hot static inline int
meta_iterator_search(MetaIterator* self, Row* row)
{
	int begin = 0;
	int end   = self->meta->regions - 1;
	while (begin != end)
	{
		auto mid = begin + (end - begin) / 2;
		auto ref = meta_region(self->meta, self->meta_data, mid);
		auto at  = meta_region_max(ref);
		if (compare(self->keys, at, row) < 0)
			begin = mid + 1;
		else
			end = mid;
	}
	if (unlikely(end >= (int)self->meta->regions))
		end = self->meta->regions - 1;
	return end;
}

hot static inline void
meta_iterator_open(MetaIterator* self,
                   Meta*         meta,
                   Buf*          meta_data,
                   Keys*         keys,
                   Row*          row)
{
	self->meta      = meta;
	self->meta_data = meta_data;
	self->keys      = keys;
	self->current   = NULL;
	self->pos       = 0;
	if (unlikely(meta == NULL || meta->regions == 0))
		return;

	if (row == NULL)
	{
		self->current = meta_region(meta, meta_data, 0);
		return;
	}
	self->pos = meta_iterator_search(self, row);

	auto ref = meta_region(self->meta, self->meta_data, self->pos);
	auto at  = meta_region_max(ref);
	if (compare(self->keys, at, row) < 0)
		self->pos++;
	if (unlikely(self->pos >= (int)meta->regions))
		return;

	self->current = meta_region(meta, meta_data, self->pos);
}

static inline bool
meta_iterator_has(MetaIterator* self)
{
	return self->current != NULL;
}

static inline MetaRegion*
meta_iterator_at(MetaIterator* self)
{
	return self->current;
}

static inline void
meta_iterator_next(MetaIterator* self)
{
	self->pos++;
	self->current = NULL;
	if (unlikely(self->pos >= (int)self->meta->regions))
		return;
	self->current = meta_region(self->meta, self->meta_data, self->pos);
}
