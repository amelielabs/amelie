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

typedef struct Flats Flats;

struct Flats
{
	Buf list;
};

always_inline static inline Flat*
flats_match(Flat* at, uint32_t timeline)
{
	// columns[flat_column_order] = head
	while (at)
	{
		if (at->timeline == timeline)
			return at;
		at = at->timeline_next;
	}
	return NULL;
}

always_inline static inline Flat*
flats_at(Flats* self, Column* column, uint32_t timeline)
{
	// columns[flat_column_order] = head
	const auto at = ((Flat**)self->list.start)[column->order_flat];
	return flats_match(at, timeline);
}

static inline void
flats_init(Flats* self)
{
	buf_init(&self->list);
}

static inline bool
flats_empty(Flats* self)
{
	return buf_empty(&self->list);
}

static inline int64_t
flats_size(Flats* self)
{
	int64_t size = 0;
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
	{
		auto at = *pos;
		while (at)
		{
			size += storage_size(&at->storage);
			at = at->timeline_next;
		}
	}
	return size;
}

static inline void
flats_free(Flats* self)
{
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
	{
		auto at = *pos;
		while (at)
		{
			auto next = at->timeline_next;
			flat_free(at);
			at = next;
		}
	}
	buf_free(&self->list);
}

static inline void
flats_retention(Flats* self, uint32_t wm)
{
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
	{
		auto at = *pos;
		while (at)
		{
			flat_retention(at, wm);
			at = at->timeline_next;
		}
	}
}

static inline void
flats_add_column(Flats* self)
{
	// extend the column list
	auto at = (Flat**)buf_emplace(&self->list, sizeof(Flat*));
	*at = NULL;
}

static inline void
flats_add(Flats* self, Flat* flat)
{
	// columns[flat_column_order] = head
	auto at = ((Flat**)self->list.start) + flat->column->order_flat;
	if (! *at)
	{
		*at = flat;
		return;
	}
	auto prev = *at;
	while (prev->timeline_next && prev->timeline < flat->timeline)
		prev = prev->timeline_next;
	flat->timeline_next = prev->timeline_next;
	prev->timeline_next = flat;
}

static inline void
flats_remove(Flats* self, Flat* flat)
{
	auto at = ((Flat**)self->list.start) + flat->column->order_flat;
	if (*at == flat)
	{
		*at = (*at)->timeline_next;
		return;
	}
	auto prev = *at;
	while (prev->timeline_next && prev->timeline_next != flat)
		prev = prev->timeline_next;
	if (! prev->timeline_next)
		return;
	prev->timeline_next = flat->timeline_next;
	flat->timeline_next = NULL;
}
