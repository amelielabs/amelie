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

typedef struct Retention Retention;

struct Retention
{
	int count;
};

static inline void
retention_init(Retention* self)
{
	self->count = INT_MAX;
}

static inline void
retention_set(Retention* self, int size_page, int64_t size)
{
	auto count = INT_MAX;
	if (size > 0)
		count = (size + size_page - 1) / size_page;
	self->count = count;
}
