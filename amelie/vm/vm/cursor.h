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
	CURSOR_SET,
	CURSOR_MERGE,
	CURSOR_MAX
};

struct Cursor
{
	int           type;
	// table
	Table*        table;
	Part*         part;
	Iterator*     it;
	// set/merge
	int           r;
	SetIterator   set_it;
	MergeIterator merge_it;
	// upsert state
	Row*          ref;
	int           ref_pos;
	int           ref_count;
	// limit/offset
	int64_t       limit;
	int64_t       offset;
};

struct CursorMgr
{
	Cursor cursor[16];
};

static inline void
cursor_init(Cursor* self)
{
	self->type      = CURSOR_NONE;
	self->table     = NULL;
	self->part      = NULL;
	self->it        = NULL;
	self->r         = 0;
	self->ref_pos   = 0;
	self->ref_count = 0;
	self->ref       = NULL;
	self->limit     = 0;
	self->offset    = 0;
	set_iterator_init(&self->set_it);
	merge_iterator_init(&self->merge_it);
}

static inline void
cursor_reset(Cursor* self)
{
	self->type      = CURSOR_NONE;
	self->table     = NULL;
	self->part      = NULL;
	self->r         = 0;
	self->ref_pos   = 0;
	self->ref_count = 0;
	self->limit     = 0;
	self->offset    = 0;
	if (self->ref)
	{
		row_free(self->ref);
		self->ref = NULL;
	}
	if (self->it)
	{
		iterator_close(self->it);
		self->it = NULL;
	}
	set_iterator_init(&self->set_it);
	merge_iterator_reset(&self->merge_it);
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
		merge_iterator_free(&cursor->merge_it);
	}
}

static inline Cursor*
cursor_mgr_of(CursorMgr* self, int id)
{
	assert(id < 16);
	return &self->cursor[id];
}
