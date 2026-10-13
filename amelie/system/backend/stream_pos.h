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
	uint32_t part;
	uint32_t page;
	uint32_t page_offset;
};

static inline void
stream_pos_init(StreamPos* self)
{
	self->part        = 0;
	self->page        = 0;
	self->page_offset = 0;
}

static inline bool
stream_pos_empty(StreamPos* self)
{
	return self->part        == 0 &&
	       self->page        == 0 &&
	       self->page_offset == 0;
}

static inline int
stream_pos_read(StreamPos* self, Str* spec)
{
	// <part>:<page>:<offset>
	Str pos = *spec;

	// part
	Str str;
	if (! str_split(&pos, &str, ':'))
		return -1;
	str_advance(&pos, str_size(&str) + 1);
	uint64_t value;
	if (str_u64(&str, &value) == -1 || value > UINT32_MAX)
		return -1;
	self->part = value;

	// page
	if (! str_split(&pos, &str, ':'))
		return -1;
	str_advance(&pos, str_size(&str) + 1);
	if (str_u64(&str, &value) == -1 || value > UINT32_MAX)
		return -1;
	self->page = value;

	// page_offset
	if (str_u64(&pos, &value) == -1 || value > UINT32_MAX)
		return -1;
	self->page_offset = value;
	return 0;
}

static inline StreamPos*
stream_pos_list_find(Buf* self, uint32_t part)
{
	auto pos = (StreamPos*)self->start;
	auto end = (StreamPos*)self->position;
	for (; pos < end; pos++)
	{
		if (pos->part == part)
			return pos;
	}
	return NULL;
}

static inline int
stream_pos_list_read(Buf* self, Str* spec)
{
	// part:page:offset[, ...]
	Str pos = *spec;
	Str str;
	for (;;)
	{
		auto has_more = str_split(&pos, &str, ',');
		auto at = (StreamPos*)buf_emplace(self, sizeof(StreamPos));
		if (stream_pos_read(at, &str) == -1)
			return -1;

		// ensure partition is not redefined
		if (stream_pos_list_find(self, at->part) != at)
			return -1;

		if (! has_more)
			break;

		str_advance(&pos, str_size(&str) + 1);
	}
	return 0;
}
