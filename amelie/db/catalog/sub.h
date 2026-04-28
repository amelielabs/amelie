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
	Catalog*   catalog;
	SubConfig* config;
};

bool sub_create(Catalog*, Tr*, SubConfig*, bool);
bool sub_drop(Catalog*, Tr*, Str*, Str*, bool);
void sub_drop_of(Catalog*, Tr*, Sub*);
bool sub_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool sub_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);

always_inline static inline Sub*
sub_of(Rel* self)
{
	return (Sub*)self;
}
