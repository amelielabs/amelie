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
} packed;

static inline void
uuid_init(Uuid* self)
{
	self->a = 0;
	self->b = 0;
}

static inline bool
uuid_empty(Uuid* self)
{
	return self->a == 0 && self->b == 0;
}

hot static inline int
uuid_compare(Uuid* l, Uuid* r)
{
	auto rc = compare_uint64(l->a, r->a);
	if (rc != 0)
		return rc;
	return compare_uint64(l->b, r->b);
}

static inline void
uuid_generate(Uuid* self, Random* random)
{
	self->a = random_generate(random);
	self->b = random_generate(random);
}

int  uuid_set_nothrow(Uuid*, Str*);
void uuid_set(Uuid*, Str*);
void uuid_get(Uuid*, char*, int);
void uuid_get_short(Uuid*, char*, int);
