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
	// <page>:<offset>
	Str pos = *spec;

	// page
	Str str;
	if (! str_split(&pos, &str, ':'))
		return -1;
	str_advance(&pos, str_size(&str) + 1);
	uint64_t value;
	if (str_u64(&str, &value) == -1 || value > UINT32_MAX)
		return -1;
	self->page = value;

	// page_offset
	if (str_u64(&pos, &value) == -1 || value > UINT32_MAX)
		return -1;
	self->page_offset = value;
	return 0;
}
