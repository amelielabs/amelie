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

typedef struct Calls Calls;

struct Calls
{
	Local*    local;
	CodeData* data;
	Buf       context;
};

void calls_init(Calls*);
void calls_free(Calls*);
void calls_prepare(Calls*, Local*, CodeData*);
void calls_reset(Calls*);

static inline void**
calls_at(Calls* self, int id)
{
	assert(buf_size(&self->context) > 0);
	auto list = (void**)self->context.start;
	return &list[id];
}
