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

typedef struct Rel Rel;

typedef void (*RelFree)(Rel*, bool);

typedef enum
{
	REL_UNDEF,
	REL_USER,
	REL_STORAGE,
	REL_TABLE,
	REL_BRANCH,
	REL_UDF,
	REL_TOPIC,
	REL_SUBSCRIPTION,
	REL_LOCK,
	REL_SYSTEM
} RelType;

struct Rel
{
	Str*     user;
	Str*     name;
	RelType  type;
	Grants*  grants;
	RelFree  free_function;
	Spinlock lock;
	uint64_t lock_order;
	int      lock_set[LOCK_MAX];
	List     lock_wait;
	int      lock_wait_count;
	List     link;
};

static inline void
rel_init(Rel* self, RelType type)
{
	self->user            = NULL;
	self->name            = NULL;
	self->type            = type;
	self->grants          = NULL;
	self->free_function   = NULL;
	self->lock_order      = 0;
	self->lock_wait_count = 0;
	spinlock_init(&self->lock);
	memset(self->lock_set, 0, sizeof(self->lock_set));
	list_init(&self->lock_wait);
	list_init(&self->link);
}

static inline void
rel_set_user(Rel* self, Str* user)
{
	self->user = user;
}

static inline void
rel_set_name(Rel* self, Str* name)
{
	self->name = name;
}

static inline void
rel_set_grants(Rel* self, Grants* grants)
{
	self->grants = grants;
}

static inline void
rel_set_free_function(Rel* self, RelFree func)
{
	self->free_function = func;
}

static inline void
rel_set_rsn(Rel* self, uint64_t value)
{
	self->lock_order = value;
}

static inline void
rel_free(Rel* self)
{
	spinlock_free(&self->lock);
	if (self->free_function)
		self->free_function(self, false);
}

static inline void
rel_drop(Rel* self)
{
	spinlock_free(&self->lock);
	if (self->free_function)
		self->free_function(self, true);
}

static inline void
rel_permission_error(Rel* self)
{
	error("relation '%.*s.%.*s': permission denied",
	      str_size(self->user), str_of(self->user),
	      str_size(self->name), str_of(self->name));
}
