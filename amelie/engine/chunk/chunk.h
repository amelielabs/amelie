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

typedef struct Chunk Chunk;

struct Chunk
{
	Id      id;
	int     state;
	bool    refresh;
	Span    span;
	Buf     span_data;
	File    file;
	Source* source;
};

static inline void
chunk_set(Chunk* self, int state)
{
	self->state |= state;
}

static inline void
chunk_unset(Chunk* self, int state)
{
	self->state &= ~state;
}

static inline bool
chunk_has(Chunk* self, int state)
{
	return (self->state & state) > 0;
}

Chunk*
chunk_allocate(Source*, Id*);
void chunk_free(Chunk*);
void chunk_open(Chunk*, int, bool);
void chunk_create(Chunk*, int);
void chunk_delete(Chunk*, int);
void chunk_rename(Chunk*, int, int);
