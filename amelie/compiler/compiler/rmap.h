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
	uint8_t* map;
	int      map_max;
	Buf      buf;
};

static inline void
rmap_init(Rmap* self)
{
	self->map     = NULL;
	self->map_max = 64;
	buf_init(&self->buf);
}

static inline void
rmap_free(Rmap* self)
{
	buf_free(&self->buf);
}

static inline void
rmap_prepare(Rmap* self)
{
	if (self->map)
		return;
	self->map = buf_claim(&self->buf, self->map_max);
	memset(self->map, TYPE_NULL, self->map_max);
}

static inline void
rmap_reset(Rmap* self)
{
	if (self->map)
		memset(self->map, TYPE_NULL, self->map_max);
}

hot static inline int
rmap_at(Rmap* self, int r)
{
	return self->map[r];
}

hot static inline int
rmap_pin(Rmap* self, int type)
{
	for (int r = 0; r < self->map_max; r++)
	{
		if (! self->map[r])
		{
			self->map[r] = type;
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
	self->map[r] = TYPE_NULL;
}
