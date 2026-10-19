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

typedef struct Lockable  Lockable;
typedef struct Lockables Lockables;

enum
{
	RES_CATALOG,
	RES_DDL,
	RES_CHECKPOINT,
	RES_BP_QUERY,
	RES_MAX
};

struct Lockable
{
	Resource   res;
	bool       bp;
	atomic_u32 bp_refs;
};

struct Lockables
{
	Lockable* list;
};

static inline void
lockable_init(Lockable* self, char* name, bool bp)
{
	self->bp      = bp;
	self->bp_refs = 0;
	resource_init(&self->res, name);
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
lockables_init(Lockables* self)
{
	self->list = am_malloc(sizeof(Lockable) * RES_MAX);
	lockable_init(&self->list[RES_CATALOG], "catalog", false);
	lockable_init(&self->list[RES_DDL], "ddl", false);
	lockable_init(&self->list[RES_CHECKPOINT], "checkpoint", false);

	// breakpoints
	lockable_init(&self->list[RES_BP_QUERY], "bp_query", true);
}

static inline void
lockables_free(Lockables* self)
{
	for (auto i = 0; i < RES_MAX; i++)
		resource_free(&self->list[i].res);
	am_free(self->list);
	self->list = NULL;
}

static inline Lockable*
lockables_find(Lockables* self, Str* name)
{
	for (auto i = 0; i < RES_MAX; i++)
	{
		auto ref = &self->list[i];
		if (str_is_cstr(name, ref->res.name))
			return ref;
	}
	return NULL;
}
