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

typedef struct Branch Branch;

struct Branch
{
	Rel           rel;
	BranchConfig* config;
	Table*        table;
};

bool branch_create(Catalog*, Tr*, BranchConfig*, bool);
bool branch_drop(Catalog*, Tr*, Str*, Str*, bool);
void branch_drop_of(Catalog*, Tr*, Branch*);
bool branch_rename(Catalog*, Tr*, Str*, Str*, Str*, Str*, bool);
bool branch_grant(Catalog*, Tr*, Str*, Str*, Str*, bool, uint32_t, bool);

always_inline static inline Branch*
branch_of(Rel* self)
{
	return (Branch*)self;
}
