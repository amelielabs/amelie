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

typedef struct Clone Clone;

struct Clone
{
	Rel          rel;
	CloneConfig* config;
	Table*       table;
};

bool clone_create(Catalog*, Tr*, CloneConfig*, bool);

always_inline static inline Clone*
clone_of(Rel* self)
{
	return (Clone*)self;
}
