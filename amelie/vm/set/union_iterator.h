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

typedef struct UnionIterator UnionIterator;

typedef Value* (*UnionStep)(UnionIterator*);

struct UnionIterator
{
	StoreIterator  it;
	StoreIterator* current_it;
	UnionStep      step;
	Buf*           list;
	int            list_count;
	int64_t        limit;
	Union*         ref;
};

always_inline static inline UnionIterator*
union_iterator_of(StoreIterator* self)
{
	return (UnionIterator*)self;
}

hot static inline Value*
union_iterator_step_all(UnionIterator* self)
{
	auto list = (StoreIterator**)self->list->start;
	if (self->current_it)
	{
		store_iterator_next(self->current_it);
		self->current_it = NULL;
	}

	StoreIterator* min_iterator = NULL;
	Value*         min = NULL;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = list[pos];
		auto row = store_iterator_at(current);
		if (! row)
			continue;
		min_iterator = current;
		min = row;
		break;
	}

	self->current_it = min_iterator;
	return min;
}

hot static inline Value*
union_iterator_step(UnionIterator* self)
{
	auto list = (StoreIterator**)self->list->start;
	if (self->current_it)
	{
		store_iterator_next(self->current_it);
		self->current_it = NULL;
	}

	StoreIterator* min_iterator = NULL;
	Value*         min = NULL;
	for (auto pos = 0; pos < self->list_count; pos++)
	{
		auto current = list[pos];
		auto row = store_iterator_at(current);
		if (! row)
			continue;

		if (min == NULL)
		{
			min_iterator = current;
			min = row;
			continue;
		}

		auto rc = store_compare(&self->ref->store, min, row);
		switch (rc) {
		case 0:
			break;
		case 1:
			min_iterator = current;
			min = row;
			break;
		case -1:
			break;
		}
	}

	self->current_it = min_iterator;
	return min;
}

hot static inline Value*
union_iterator_step_distinct(UnionIterator* self)
{
	Value* first;
	if (likely(self->current_it))
		first = store_iterator_at(self->current_it);
	else
		first = union_iterator_step(self);
	if (unlikely(! first))
		return NULL;

	for (;;)
	{
		auto next = union_iterator_step(self);
		if (unlikely(! next))
			break;
		if (! store_compare(&self->ref->store, first, next))
			continue;
		break;
	}
	return first;
}

hot static inline Value*
union_iterator_step_join(UnionIterator* self)
{
	// return unique records which matches all sets
	auto list = (StoreIterator**)self->list->start;
	if (self->current_it)
	{
		// advance all
		for (auto pos = 0; pos < self->list_count; pos++)
			store_iterator_next(list[pos]);
		self->current_it = NULL;
	}

	for (;;)
	{
		auto           matches = 0;
		StoreIterator* min_iterator = NULL;
		Value*         min = NULL;
		for (auto pos = 0; pos < self->list_count; pos++)
		{
			auto current = list[pos];
			auto row = store_iterator_at(current);
			if (! row)
			{
				// stop on any end
				return NULL;
			}

			if (min == NULL)
			{
				min_iterator = current;
				min = row;
				continue;
			}

			auto rc = store_compare(&self->ref->store, min, row);
			switch (rc) {
			case 0:
				matches++;
				break;
			case 1:
				min_iterator = current;
				min = row;
				break;
			case -1:
				break;
			}
		}

		// match
		if (matches == self->list_count)
		{
			self->current_it = min_iterator;
			return min;
		}

		// iterate min
		store_iterator_next(min_iterator);
	}

	// unreach
	return NULL;
}

hot static inline Value*
union_iterator_step_except(UnionIterator* self)
{
	// return unique records which exists in the first set only
	auto list = (StoreIterator**)self->list->start;
	if (self->current_it)
	{
		store_iterator_next(self->current_it);
		self->current_it = NULL;
	}

	for (;;)
	{
		auto           matches = 0;
		StoreIterator* min_iterator = NULL;
		Value*         min = NULL;
		for (auto pos = 0; pos < self->list_count; pos++)
		{
			auto current = list[pos];
			auto row = store_iterator_at(current);
			if (! row)
			{
				// stop on first set end
				if (pos == 0)
					return NULL;
				continue;
			}

			if (min == NULL)
			{
				min_iterator = current;
				min = row;
				continue;
			}

			auto rc = store_compare(&self->ref->store, min, row);
			switch (rc) {
			case 0:
				matches++;
				break;
			case 1:
				min_iterator = current;
				min = row;
				break;
			case -1:
				break;
			}

			// unique first set record
			if (!matches && min_iterator == list[0])
			{
				self->current_it = min_iterator;
				return min;
			}

			// iterate min
			store_iterator_next(min_iterator);
		}
	}

	// unreach
	return NULL;
}

hot static inline void
union_iterator_next(StoreIterator* arg)
{
	// apply limit
	auto self = union_iterator_of(arg);
	arg->current = NULL;

	if (self->limit-- <= 0)
	{
		self->current_it = NULL;
		return;
	}

	// iterate
	arg->current = self->step(self);
}

static inline void
union_iterator_close(StoreIterator* arg)
{
	auto self = union_iterator_of(arg);
	if (self->list)
	{
		auto it = (StoreIterator**)self->list->start;
		for (auto i = 0; i < self->list_count; i++)
			store_iterator_close(it[i]);
		buf_free(self->list);
	}
	am_free(arg);
}

hot static inline void
union_iterator_open(UnionIterator* self)
{
	auto ref = self->ref;
	if (! ref->list_count)
		return;

	// prepare iterators
	self->list = buf_create();
	buf_reserve(self->list, sizeof(StoreIterator*) * ref->list_count);

	auto list = (Store**)ref->list->start;
	auto it   = (StoreIterator**)self->list->start;
	for (auto i = 0; i < ref->list_count; i++)
		it[self->list_count++] = store_iterator(list[i]);

	// set to position
	int64_t offset = ref->offset;
	if (offset == 0)
	{
		self->limit = ref->limit;
		union_iterator_next(&self->it);
		return;
	}

	union_iterator_next(&self->it);

	// apply offset
	while (offset-- > 0 && self->it.current)
		union_iterator_next(&self->it);

	self->limit = ref->limit - 1;
	if (self->limit < 0)
		self->it.current = NULL;
}

static inline StoreIterator*
union_iterator_allocate(Union* ref)
{
	UnionIterator* self = am_malloc(sizeof(*self));
	self->it.next       = union_iterator_next;
	self->it.close      = union_iterator_close;
	self->it.current    = NULL;
	self->current_it    = NULL;
	self->step          = NULL;
	self->list          = NULL;
	self->list_count    = 0;
	self->limit         = INT64_MAX;
	self->ref           = ref;
	errdefer(union_iterator_close, self);

	// set step function
	switch (ref->type) {
	case UNION_ALL:
		self->step = union_iterator_step_all;
		break;
	case UNION_ORDERED:
		self->step = union_iterator_step;
		break;
	case UNION_ORDERED_DISTINCT:
		self->step = union_iterator_step_distinct;
		break;
	case UNION_JOIN:
		self->step = union_iterator_step_join;
		break;
	case UNION_EXCEPT:
		self->step = union_iterator_step_except;
		break;
	}

	union_iterator_open(self);
	return &self->it;
}
