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

typedef struct Fns Fns;

struct Fns
{
	Local*    local;
	CodeData* data;
	Buf       context;
};

void fns_init(Fns*);
void fns_free(Fns*);
void fns_prepare(Fns*, Local*, CodeData*);
void fns_reset(Fns*);

static inline void**
fns_at(Fns* self, int id)
{
	assert(buf_size(&self->context) > 0);
	auto list = (void**)self->context.start;
	return &list[id];
}
