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

typedef struct Interval Interval;

struct Interval
{
	int32_t m;
	int32_t d;
	int64_t us;
};

static inline void
interval_init(Interval* self)
{
	self->m  = 0;
	self->d  = 0;
	self->us = 0;
}

static inline void
interval_set(Interval* self, int m, int d, uint64_t us)
{
	self->m  = m;
	self->d  = d;
	self->us = us;
}

void     interval_read(Interval*, Str*);
int      interval_write(Interval*, char*, int);
int      interval_compare(Interval*, Interval*);
void     interval_add(Interval*, Interval*, Interval*);
void     interval_sub(Interval*, Interval*, Interval*);
void     interval_neg(Interval*, Interval*);
void     interval_trunc(Interval*, Str*);
uint64_t interval_extract(Interval*, Str*);
