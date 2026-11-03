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

typedef struct Stream Stream;

struct Stream
{
	HeapIterator it;
	Sub          sub;
	Buf          data;
	StreamPos    pos;
	Part*        part;
};

void stream_init(Stream*, StreamPos*, Part*);
void stream_free(Stream*);
int  stream_next(Stream*);

static inline void
stream_attach(Stream* self)
{
	// add to the wait list
	subs_add(&self->part->subs, &self->sub);
}

static inline bool
stream_detach(Stream* self)
{
	auto cancel = self->sub.cancel;
	if (self->sub.wait)
		subs_remove(&self->part->subs, &self->sub);
	return cancel;
}
