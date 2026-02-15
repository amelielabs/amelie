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
	Relation   rel;
	Str        rel_name;
	bool       bp;
	atomic_u32 bp_refs;
};

struct LockableMgr
{
	Lockable* list;
};

static inline void
lockable_init(Lockable* self, Rel id, const char* name, bool bp)
{
	self->bp      = bp;
	self->bp_refs = 0;
	str_set_cstr(&self->rel_name, name);
	auto rel = &self->rel;
	relation_init(rel);
	relation_set_rsn(rel, id);
	relation_set_name(rel, &self->rel_name);
}

static inline void
lockable_breakpoint_ref(Lockable* self)
{
	atomic_u32_inc(&self->bp_refs);
}

static inline void
lockable_breakpoint_unref(Lockable* self)
{
	atomic_u32_dec(&self->bp_refs);
}

static inline void
lockable_mgr_init(LockableMgr* self)
{
	self->list = am_malloc(sizeof(Lockable) * REL_MAX);
	lockable_init(&self->list[REL_CATALOG], REL_CATALOG, "catalog", false);
	lockable_init(&self->list[REL_DDL], REL_DDL, "ddl", false);
}

static inline void
lockable_mgr_free(LockableMgr* self)
{
	for (auto i = 0; i < REL_MAX; i++)
		relation_free(&self->list[i].rel);
	am_free(self->list);
	self->list = NULL;
}

static inline Lockable*
lockable_mgr_find(LockableMgr* self, Str* name)
{
	for (auto i = 0; i < REL_MAX; i++)
	{
		auto ref = &self->list[i];
		if (str_compare(ref->rel.name, name))
			return ref;
	}
	return NULL;
}
