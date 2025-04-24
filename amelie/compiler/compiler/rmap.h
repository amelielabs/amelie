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
	Buf buf;
	int count;
};

always_inline static inline int
rmap_at(Rmap* self, int r)
{
	return self->buf.start[r];
}

always_inline inline void
rmap_assign(Rmap* self, int r, int type)
{
	self->buf.start[r] = type;
}

static inline void
rmap_init(Rmap* self)
{
	self->count = 0;
	buf_init(&self->buf);
}

static inline void
rmap_free(Rmap* self)
{
	buf_free(&self->buf);
}

static inline void
rmap_reset(Rmap* self)
{
	self->count = 0;
	buf_reset(&self->buf);
}

hot static inline int
rmap_pin(Rmap* self, int type)
{
	auto map = &self->buf;
	for (auto r = 0; r < self->count; r++)
	{
		if (rmap_at(self, r) != 0xff)
			continue;
		rmap_assign(self, r, type);
		return r;
	}
	buf_reserve(map, sizeof(uint8_t));
	auto r = self->count;
	rmap_assign(self, r, type);
	buf_advance(map, sizeof(uint8_t));
	self->count++;
	return r;
}

static inline void
rmap_unpin(Rmap* self, int r)
{
	rmap_assign(self, r, 0xff);
}
