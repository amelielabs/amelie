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

typedef struct Uuid Uuid;

#define UUID_SZ 37

struct Uuid
{
	uint64_t a;
	uint64_t b;
};

void uuid_init(Uuid*);
void uuid_generate(Uuid*, Random*);
void uuid_to_string(Uuid*, char*, int);
void uuid_from_string(Uuid*, Str*);
int  uuid_from_string_nothrow(Uuid*, Str*);

static inline bool
uuid_empty(Uuid* self)
{
	return self->a == 0 && self->b == 0;
}

static inline bool
uuid_compare(Uuid *l, Uuid *r)
{
	return l->a == r->a && l->b == r->b;
}
