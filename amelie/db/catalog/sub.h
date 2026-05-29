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

typedef struct Catalog Catalog;
typedef struct Sub     Sub;

struct Sub
{
	Rel        rel;
	Rel*       rel_on;
	CdcSlot    slot;
	Catalog*   catalog;
	SubConfig* config;
};

bool sub_create(Catalog*, Tr*, SubConfig*, bool);

always_inline static inline Sub*
sub_of(Rel* self)
{
	return (Sub*)self;
}
