#pragma once

//
// monotone
//
// SQL OLTP database
//

typedef struct RowGc RowGc;

struct RowGc
{
	bool active;
	int  list_count;
	Buf  list;
};

static inline void
row_gc_init(RowGc* self)
{
	self->active     = false;
	self->list_count = 0;
	buf_init(&self->list);
}

static inline void
row_gc_reset(RowGc* self)
{
	if (self->list_count == 0)
		return;
	Row** list = (Row**)self->list.start;
	for (int i = 0; i < self->list_count; i++)
		row_free(list[i]);
	self->list_count = 0;
	buf_reset(&self->list);
}

static inline void
row_gc_free(RowGc* self)
{
	row_gc_reset(self);
	buf_free(&self->list);
}

static inline void
row_gc_start(RowGc* self)
{
	self->active = true;
}

static inline void
row_gc_stop(RowGc* self)
{
	row_gc_reset(self);
	self->active = false;
}

hot static inline void
row_gc(RowGc* self, Row* row)
{
	if (! self->active)
	{
		row_free(row);
		return;
	}
	buf_write(&self->list, &row, sizeof(Row*));
	self->list_count++;
}
