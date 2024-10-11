#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Cursor    Cursor;
typedef struct CursorMgr CursorMgr;

enum
{
	CURSOR_NONE,
	CURSOR_TABLE,
	CURSOR_OBJ,
	CURSOR_ARRAY,
	CURSOR_SET,
	CURSOR_MERGE,
	CURSOR_GROUP
};

struct Cursor
{
	int           type;
	// partition
	Table*        table;
	Keys*         keys;
	Part*         part;
	Iterator*     it;
	// ref
	int           ref_pos;
	int           ref_count;
	uint8_t*      ref;
	uint8_t*      ref_key;
	// expr
	int           r;
	bool          cte;
	uint8_t*      obj_pos;
	// set
	SetIterator   set_it;
	// merge
	MergeIterator merge_it;
	// group
	Group*        group;
	int           group_pos;
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
	self->keys      = NULL;
	self->part      = NULL;
	self->it        = NULL;
	self->ref_pos   = 0;
	self->ref_count = 0;
	self->ref       = NULL;
	self->ref_key   = NULL;
	self->r         = 0;
	self->cte       = false;
	self->obj_pos   = NULL;
	self->group     = NULL;
	self->group_pos = 0;
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
	self->keys      = NULL;
	self->part      = NULL;
	self->ref_pos   = 0;
	self->ref_count = 0;
	self->ref       = NULL;
	self->ref_key   = NULL;
	self->r         = 0;
	self->cte       = false;
	self->obj_pos   = NULL;
	self->group     = NULL;
	self->group_pos = 0;
	self->limit     = 0;
	self->offset    = 0;
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
