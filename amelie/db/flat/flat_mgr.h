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

typedef struct FlatMgr FlatMgr;

struct FlatMgr
{
	Buf list;
	int list_count;
};

always_inline static inline Flat*
flat_mgr_at(FlatMgr* self, Column* column)
{
	return ((Flat**)self->list.start)[column->order_flat];
}

static inline void
flat_mgr_init(FlatMgr* self)
{
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
flat_mgr_free(FlatMgr* self)
{
	auto pos = (Flat**)self->list.start;
	auto end = (Flat**)self->list.position;
	for (; pos < end; pos++)
		flat_free(*pos);
	buf_free(&self->list);
	self->list_count = 0;
}

static inline void
flat_mgr_add(FlatMgr* self, Flat* flat)
{
	buf_write(&self->list, &flat, sizeof(flat));
	self->list_count++;
}
