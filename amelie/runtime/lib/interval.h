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
} packed;

static inline void
interval_init(Interval* self)
{
	self->m  = 0;
	self->d  = 0;
	self->us = 0;
}

void     interval_set(Interval*, Str*);
int      interval_get(Interval*, char*, int);
int      interval_compare(Interval*, Interval*);
void     interval_add(Interval*, Interval*, Interval*);
void     interval_sub(Interval*, Interval*, Interval*);
void     interval_neg(Interval*, Interval*);
void     interval_trunc(Interval*, Str*);
uint64_t interval_extract(Interval*, Str*);
