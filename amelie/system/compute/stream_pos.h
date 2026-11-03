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

typedef struct StreamPos StreamPos;

struct StreamPos
{
	uint32_t page;
	uint32_t page_offset;
};

static inline void
stream_pos_init(StreamPos* self)
{
	self->page        = 0;
	self->page_offset = 0;
}

static inline bool
stream_pos_empty(StreamPos* self)
{
	return self->page        == 0 &&
	       self->page_offset == 0;
}

static inline int
stream_pos_read(StreamPos* self, Str* spec)
{
	// <value>
	uint64_t value;
	if (str_u64(spec, &value) == -1)
		return -1;

	auto size = (2 * 1024 * 1024);
	self->page = value / size;
	self->page_offset = value % size;

	if (self->page > (1 << 17))
		return -1;
	if (self->page_offset > (1 << 21))
		return -1;

	return 0;
}
