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
	int list_count;
	Buf list;
};

always_inline static inline Flat*
flats_at(Flats* self, Column* column)
{
	return ((Flat**)self->list.start)[column->order_flat];
}

static inline void
flats_init(Flats* self)
{
	self->list_count = 0;
	buf_init(&self->list);
}

static inline bool
flats_empty(Flats* self)
{
	return !self->list_count;
}

static inline int64_t
flats_size(Flats* self)
{
	int64_t size = 0;
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
		size += storage_size(&(*pos)->storage);
	return size;
}

static inline void
flats_free(Flats* self)
{
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
		flat_free(*pos);
	buf_free(&self->list);
	self->list_count = 0;
}

static inline void
flats_add(Flats* self, Flat* flat)
{
	buf_write(&self->list, &flat, sizeof(flat));
	self->list_count++;
}
