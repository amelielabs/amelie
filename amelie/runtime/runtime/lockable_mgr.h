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

typedef struct Lockable    Lockable;
typedef struct LockableMgr LockableMgr;

typedef enum
{
	REL_CATALOG,
	REL_DDL,
	REL_MAX
} Rel;

struct Lockable
{
	Relation rel;
	Str      rel_name;
};

struct LockableMgr
{
	Lockable* rels;
};

static inline void
lockable_init(Lockable* self, Rel id, const char* name)
{
	str_set_cstr(&self->rel_name, name);
	auto rel = &self->rel;
	relation_init(rel);
	relation_set_rsn(rel, id);
	relation_set_name(rel, &self->rel_name);
}

static inline void
lockable_mgr_init(LockableMgr* self)
{
	self->rels = am_malloc(sizeof(Lockable) * REL_MAX);
	lockable_init(&self->rels[REL_CATALOG], REL_CATALOG, "catalog");
	lockable_init(&self->rels[REL_DDL], REL_DDL, "ddl");
}

static inline void
lockable_mgr_free(LockableMgr* self)
{
	for (auto i = 0; i < REL_MAX; i++)
		relation_free(&self->rels[i].rel);
	am_free(self->rels);
	self->rels = NULL;
}
