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

typedef struct Limit Limit;

struct Limit
{
	_Atomic uint64_t current;
	uint64_t         limit;
	bool             enable;
	char*            name;
};

static inline void
limit_init(Limit* self, char* name)
{
	self->current = 0;
	self->limit   = 0;
	self->enable  = false;
	self->name    = name;
}

static inline void
limit_reset(Limit* self)
{
	self->current = 0;
	self->limit   = 0;
	self->enable  = false;
}

static inline void
limit_set(Limit* self, bool enable, uint64_t limit)
{
	self->limit  = limit;
	self->enable = enable;
}

hot static inline bool
limit_reserve(Limit* self, uint64_t size)
{
	uint64_t current = atomic_load_explicit(&self->current, memory_order_relaxed);
	for (;;)
	{
		// global limit reached (with overflow check)
		if (unlikely(current + size < current || current + size > self->limit))
			return false;

		// atomically add total usage (if it has not changed)
		if (likely(atomic_compare_exchange_weak_explicit(&self->current, &current,
		                                                 current + size,
		                                                 memory_order_relaxed,
		                                                 memory_order_relaxed)))
			break;
	}

	return true;
}

hot static inline void
limit_add(Limit* self, uint64_t size)
{
	if (! self->enable)
		return;

	if (likely(limit_reserve(self, size)))
		return;

	error("{s} limit reached", self->name);
}

hot static inline void
limit_sub(Limit* self, uint64_t size)
{
	uint64_t current = atomic_load_explicit(&self->current, memory_order_relaxed);
	for (;;)
	{
		// atomically sub total usage (if it has not changed)
		if (likely(atomic_compare_exchange_weak_explicit(&self->current, &current,
		                                                 current - size,
		                                                 memory_order_relaxed,
		                                                 memory_order_relaxed)))
			break;
	}
}
