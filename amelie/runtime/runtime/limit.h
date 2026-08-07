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
	atomic_u64 count;
	uint64_t   limit;
};

static inline void
limit_init(Limit* self, uint64_t limit)
{
	self->count = 0;
	self->limit = limit;
}

hot static inline void
limit_add(Limit* self)
{
	if (! self->limit)
		return;

	if (atomic_u64_inc(&self->count) >= self->limit)
		error("transaction log limit reached");
}
