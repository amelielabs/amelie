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

typedef struct LockSystem LockSystem;

typedef enum
{
	SYSTEM_CATALOG,
	SYSTEM_DDL,
	SYSTEM_MAX
} LockSystemId;

struct LockSystem
{
	Relation rels[SYSTEM_MAX];
};

static inline void
lock_system_init(LockSystem* self)
{
	for (auto i = 0; i < SYSTEM_MAX; i++)
	{
		auto rel = &self->rels[i];
		relation_init(rel);
		relation_set_rsn(rel, i);
	}
}
