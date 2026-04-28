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

typedef struct Udf Udf;

typedef void (*UdfFree)(Udf*);

struct Udf
{
	Rel        rel;
	UdfFree    free;
	void*      free_arg;
	void*      data;
	UdfConfig* config;
};

bool udf_create(Catalog*, Tr*, UdfConfig*, bool);
bool udf_drop(Catalog*, Tr*, Str*, Str*, bool);
void udf_drop_of(Catalog*, Tr*, Udf*);
bool udf_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool udf_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);

always_inline static inline Udf*
udf_of(Rel* self)
{
	return (Udf*)self;
}
