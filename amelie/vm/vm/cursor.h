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

typedef struct Cursor    Cursor;
typedef struct CursorMgr CursorMgr;

enum
{
	CURSOR_NONE,
	CURSOR_TABLE,
	CURSOR_STORE,
	CURSOR_JSON,
	CURSOR_MAX
};

struct Cursor
{
	int            type;
	int            r;
	// table
	Table*         table;
	Part*          part;
	Iterator*      it;
	// store
	StoreIterator* it_store;
	// json
	uint8_t*       pos;
	int            pos_size;
	// upsert state
	Value*         ref;
	int            ref_pos;
	int            ref_count;
};

struct CursorMgr
{
	Cursor cursor[16];
};

static inline void
cursor_init(Cursor* self)
{
	self->type      = CURSOR_NONE;
	self->r         = 0;
	self->table     = NULL;
	self->part      = NULL;
	self->it        = NULL;
	self->it_store  = NULL;
	self->pos       = NULL;
	self->pos_size  = 0;
	self->ref       = NULL;
	self->ref_pos   = 0;
	self->ref_count = 0;
}

static inline void
cursor_reset(Cursor* self)
{
	self->type      = CURSOR_NONE;
	self->r         = 0;
	self->table     = NULL;
	self->part      = NULL;
	self->pos       = NULL;
	self->pos_size  = 0;
	self->ref       = NULL;
	self->ref_pos   = 0;
	self->ref_count = 0;
	if (self->it)
	{
		iterator_close(self->it);
		self->it = NULL;
	}
	if (self->it_store)
	{
		store_iterator_close(self->it_store);
		self->it_store = NULL;
	}
}

static inline void
cursor_mgr_init(CursorMgr* self)
{
	for (int i = 0; i < 16; i++)
	{
		auto cursor = &self->cursor[i];
		cursor_init(cursor);
	}
}

static inline void
cursor_mgr_reset(CursorMgr* self)
{
	for (int i = 0; i < 16; i++)
	{
		auto cursor = &self->cursor[i];
		cursor_reset(cursor);
	}
}

static inline void
cursor_mgr_free(CursorMgr* self)
{
	for (int i = 0; i < 16; i++)
	{
		auto cursor = &self->cursor[i];
		cursor_reset(cursor);
	}
}

static inline Cursor*
cursor_mgr_of(CursorMgr* self, int id)
{
	assert(id < 16);
	return &self->cursor[id];
}
