#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct Cursor    Cursor;
typedef struct CursorMgr CursorMgr;

enum
{
	CURSOR_NONE,
	CURSOR_TABLE,
	CURSOR_ARRAY,
	CURSOR_MAP,
	CURSOR_SET,
	CURSOR_MERGE,
	CURSOR_GROUP
};

struct Cursor
{
	int             type;
	int             r;
	// storage
	Table*          table;
	Def*            def;
	Storage*        storage;
	StorageIterator it;
	// ref
	uint8_t*        ref;
	uint8_t*        ref_key;
	// expr
	uint8_t*        obj_data;
	int             obj_pos;
	int             obj_count;
	// set
	SetIterator     set_it;
	// merge
	Merge*          merge;
	// group
	Group*          group;
	int             group_pos;
	// limit/offset
	int64_t         limit;
	int64_t         offset;
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
	self->def       = NULL;
	self->storage   = NULL;
	self->ref       = NULL;
	self->ref_key   = NULL;
	self->obj_data  = NULL;
	self->obj_pos   = 0;
	self->obj_count = 0;
	self->merge     = NULL;
	self->group     = NULL;
	self->group_pos = 0;
	self->limit     = 0;
	self->offset    = 0;
	storage_iterator_init(&self->it);
	set_iterator_init(&self->set_it);
}

static inline void
cursor_reset(Cursor* self)
{
	self->type      = CURSOR_NONE;
	self->r         = 0;
	self->table     = NULL;
	self->def       = NULL;
	self->storage   = NULL;
	self->ref       = NULL;
	self->ref_key   = NULL;
	self->obj_data  = NULL;
	self->obj_pos   = 0;
	self->obj_count = 0;
	self->merge     = NULL;
	self->group     = NULL;
	self->group_pos = 0;
	self->limit     = 0;
	self->offset    = 0;
	storage_iterator_reset(&self->it);
	set_iterator_init(&self->set_it);
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
	cursor_mgr_reset(self);
}

static inline Cursor*
cursor_mgr_of(CursorMgr* self, int id)
{
	assert(id < 16);
	return &self->cursor[id];
}
