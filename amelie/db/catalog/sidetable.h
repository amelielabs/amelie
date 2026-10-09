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

typedef struct Sidetable Sidetable;

struct Sidetable
{
	Rel              rel;
	SidetableConfig* config;
	Table*           table;
};

bool sidetable_create(Catalog*, Tr*, SidetableConfig*, bool);

always_inline static inline Sidetable*
sidetable_of(Rel* self)
{
	return (Sidetable*)self;
}
