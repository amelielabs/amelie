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

typedef struct Ref  Ref;
typedef struct Refs Refs;

struct Ref
{
	Ast*     ast;
	int      r;
	Targets* targets;
};

struct Refs
{
	Buf buf;
	int count;
};

always_inline static inline Ref*
refs_at(Refs* self, int order)
{
	return &((Ref*)self->buf.start)[order];
}

static inline void
refs_init(Refs* self)
{
	self->count = 0;
	buf_init(&self->buf);
}

static inline void
refs_free(Refs* self)
{
	buf_free(&self->buf);
}

static inline void
refs_reset(Refs* self)
{
	self->count = 0;
	buf_reset(&self->buf);
}

hot static inline int
refs_add(Refs* self, Targets* targets, Ast* ast, int r)
{
	if (ast)
	{
		for (auto order = 0; order < self->count; order++)
		{
			auto ref = refs_at(self, order);
			if (ref->ast == ast)
				return order;
		}
	} else
	{
		assert(r != -1);
		for (auto order = 0; order < self->count; order++)
		{
			auto ref = refs_at(self, order);
			if (ref->r == r)
				return order;
		}
	}
	auto ref = (Ref*)buf_claim(&self->buf, sizeof(Ref));
	ref->ast     = ast;
	ref->r       = r;
	ref->targets = targets;
	return self->count++;
}
