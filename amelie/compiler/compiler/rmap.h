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

typedef struct Rmap Rmap;

struct Rmap
{
	bool map[128];
	Buf  stack;
	int  stack_size;
};

static inline void
rmap_init(Rmap* self)
{
	self->stack_size = 0;
	memset(self->map, 0, sizeof(self->map));
	buf_init(&self->stack);
}

static inline void
rmap_free(Rmap* self)
{
	buf_free(&self->stack);
}

static inline void
rmap_reset(Rmap* self)
{
	self->stack_size = 0;
	memset(self->map, 0, sizeof(self->map));
	buf_reset(&self->stack);
}

hot static inline int
rmap_pin(Rmap* self)
{
	for (int r = 0; r < 128; r++)
	{
		if (! self->map[r])
		{
			self->map[r] = true;
			return r;
		}
	}
	error("failed to pin register");
	return -1;
}

static inline void
rmap_unpin(Rmap* self, int r)
{
	assert(r < 128);
	self->map[r] = false;
}

static inline void
rmap_push(Rmap* self, int r)
{
	buf_write(&self->stack, &r, sizeof(r));
	self->stack_size++;
}

static inline int
rmap_pop(Rmap* self)
{
	if (self->stack_size == 0)
		return -1;
	int r = *(int*)(self->stack.position - sizeof(int));
	buf_truncate(&self->stack, sizeof(int));
	self->stack_size--;
	return r;
}
