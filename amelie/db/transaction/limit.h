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
	uint64_t   log_size_limit;
	atomic_u64 log_size;
};

static inline void
limit_init(Limit* self, uint64_t limit)
{
	self->log_size_limit = limit;
	self->log_size       = 0;
}

static inline void
limit_reset(Limit* self, uint64_t limit)
{
	self->log_size_limit = limit;
	self->log_size       = 0;
}

hot static inline void
limit_ensure(Limit* self, Row* row)
{
	if (self->log_size_limit == 0)
		return;

	// in-memory row size
	int size = row_size(row);

	// this might be slightly inaccurate
	if (unlikely((atomic_u64_of(&self->log_size) + size) >= self->log_size_limit))
		error("transaction log limit reached");

	atomic_u64_add(&self->log_size, size);
}
