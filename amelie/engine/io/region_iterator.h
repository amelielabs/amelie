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

typedef struct RegionIterator RegionIterator;

struct RegionIterator
{
	Iterator it;
	Region*  region;
	int      pos;
	Row*     current;
	Keys*    keys;
};

hot static inline void
region_iterator_set_pos(RegionIterator* self, int pos)
{
	if (unlikely(pos < 0 || pos >= (int)self->region->count))
	{
		self->current = NULL;
		self->pos = 0;
		return;
	}
	self->current = region_row(self->region, pos);
	self->pos     = pos;
}

always_inline hot static inline int
region_iterator_compare(RegionIterator* self, int pos, Row* row)
{
	auto current = region_row(self->region, pos);
	return compare(self->keys, current, row);
}

hot static inline int
region_iterator_search(RegionIterator* self, Row* row, bool* match)
{
	int min = 0;
	int mid = 0;
	int max = self->region->count - 1;
	while (max >= min)
	{
		mid = min + (max - min) / 2;
		int rc = region_iterator_compare(self, mid, row);
		if (rc < 0) {
			min = mid + 1;
		} else
		if (rc > 0) {
			max = mid - 1;
		} else {
			*match = true;
		    return mid;
		}
	}
	return min;
}

hot static inline bool
region_iterator_open(RegionIterator* self,
                     Region*         region,
                     Keys*           keys,
                     Row*            row)
{
	self->region  = region;
	self->pos     = 0;
	self->current = NULL;
	self->keys    = keys;
	if (unlikely(self->region->count == 0))
		return false;
	bool match = false;
	int pos;
	if (row)
		pos = region_iterator_search(self, row, &match);
	else
		pos = 0;
	region_iterator_set_pos(self, pos);
	return match;
}

static inline bool
region_iterator_has(RegionIterator* self)
{
	return self->current != NULL;
}

static inline Row*
region_iterator_at(RegionIterator* self)
{
	return self->current;
}

hot static inline void
region_iterator_next(RegionIterator* self)
{
	if (unlikely(self->current == NULL))
		return;
	region_iterator_set_pos(self, self->pos + 1);
}

static inline void
region_iterator_init(RegionIterator* self)
{
	self->current = NULL;
	self->pos     = 0;
	self->region  = NULL;
	self->keys    = NULL;
	auto it = &self->it;
	it->has   = (IteratorHas)region_iterator_has;
	it->at    = (IteratorAt)region_iterator_at;
	it->next  = (IteratorNext)region_iterator_next;
	it->close = NULL;
}

static inline void
region_iterator_reset(RegionIterator* self)
{
	self->current = NULL;
	self->pos     = 0;
	self->region  = NULL;
	self->keys    = NULL;
}
