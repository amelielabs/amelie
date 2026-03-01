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

typedef struct BranchIterator BranchIterator;

struct BranchIterator
{
	Iterator       it;
	RegionIterator region_iterator;
	MetaIterator   meta_iterator;
	MetaRegion*    current;
	Branch*        branch;
	Keys*          keys;
	Reader         reader;
	bool           allocated;
};

hot static inline bool
branch_iterator_open_region(BranchIterator* self, Row* row)
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
branch_iterator_open(BranchIterator* self, Keys* keys, Branch* branch, Row* row)
{
	self->branch  = branch;
	self->keys    = keys;
	self->current = NULL;

	region_iterator_init(&self->region_iterator);
	meta_iterator_init(&self->meta_iterator);
	meta_iterator_open(&self->meta_iterator, &branch->meta, &branch->meta_data,
	                    keys, row);

	self->current = meta_iterator_at(&self->meta_iterator);
	if (self->current == NULL)
		return false;

	reader_open(&self->reader, branch);
	return branch_iterator_open_region(self, row);
}

hot static inline bool
branch_iterator_has(BranchIterator* self)
{
	return region_iterator_has(&self->region_iterator);
}

hot static inline Row*
branch_iterator_at(BranchIterator* self)
{
	return region_iterator_at(&self->region_iterator);
}

hot static inline void
branch_iterator_next(BranchIterator* self)
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
		meta_iterator_next(&self->meta_iterator);
		self->current = meta_iterator_at(&self->meta_iterator);
		if (unlikely(self->current == NULL))
			break;

		branch_iterator_open_region(self, NULL);
	}
}

static inline void
branch_iterator_free(BranchIterator* self)
{
	reader_reset(&self->reader);
	reader_free(&self->reader);
	if (self->allocated)
		am_free(self);
}

static inline void
branch_iterator_reset(BranchIterator* self)
{
	reader_reset(&self->reader);
	region_iterator_reset(&self->region_iterator);
}

static inline void
branch_iterator_init(BranchIterator* self)
{
	self->branch    = NULL;
	self->current   = NULL;
	self->allocated = false;
	reader_init(&self->reader);
	meta_iterator_init(&self->meta_iterator);
	region_iterator_init(&self->region_iterator);
	auto it = &self->it;
	it->has   = (IteratorHas)branch_iterator_has;
	it->at    = (IteratorAt)branch_iterator_at;
	it->next  = (IteratorNext)branch_iterator_next;
	it->close = (IteratorClose)branch_iterator_free;
}

static inline BranchIterator*
branch_iterator_allocate(void)
{
	auto self = (BranchIterator*)am_malloc(sizeof(BranchIterator));
	branch_iterator_init(self);
	self->allocated = true;
	return self;
}
