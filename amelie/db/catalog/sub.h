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
	CdcSlot    slot;
	Uuid       on_id;
	Cdc*       cdc;
	Catalog*   catalog;
	SubConfig* config;
};

Sub* sub_allocate(SubConfig*, Catalog*, Cdc*);

static inline Sub*
sub_of(Rel* self)
{
	return (Sub*)self;
}
