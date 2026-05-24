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

always_inline static inline Branch*
branch_of(Rel* self)
{
	return (Branch*)self;
}
