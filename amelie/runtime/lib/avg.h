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

typedef struct Avg Avg;

struct Avg
{
	union {
		int64_t sum_int;
		double  sum_double;
	};
	uint64_t count;
};

static inline void
avg_init(Avg* self)
{
	memset(self, 0, sizeof(*self));
}

static inline void
avg_add_int(Avg* self, int64_t value, int count)
{
	self->sum_int += value;
	self->count   += count;
}

static inline void
avg_add_double(Avg* self, double value, int count)
{
	self->sum_double += value;
	self->count      += count;
}

static inline int64_t
avg_int(Avg* self)
{
	return self->sum_int / self->count;
}

static inline double
avg_double(Avg* self)
{
	return self->sum_double / self->count;
}
