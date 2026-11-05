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

typedef struct AccessRel AccessRel;
typedef struct Access    Access;

struct AccessRel
{
	Rel*     rel;
	uint32_t perm;
};

struct Access
{
	int  count;
	Buf  list;
	bool ro;
};

static inline AccessRel*
access_at(Access* self, int order)
{
	return &((AccessRel*)self->list.start)[order];
}

static inline void
access_init(Access* self)
{
	self->ro    = true;
	self->count = 0;
	buf_init(&self->list);
}

static inline void
access_free(Access* self)
{
	buf_free(&self->list);
}

static inline void
access_reset(Access* self)
{
	self->ro    = true;
	self->count = 0;
	buf_reset(&self->list);
}

static inline bool
access_empty(Access* self)
{
	return !self->count;
}

static inline bool
access_ro(Access* self)
{
	return self->ro;
}

hot static inline void
access_add(Access* self, Rel* rel, uint32_t perm)
{
	if (permission_is_dml(perm))
		self->ro = false;

	// add unique relation
	for (auto i = 0; i < self->count; i++)
	{
		auto ref = access_at(self, i);
		if (ref->rel == rel)
		{
			ref->perm |= perm;
			return;
		}
	}

	auto ref  = (AccessRel*)buf_emplace(&self->list, sizeof(AccessRel));
	ref->rel  = rel;
	ref->perm = perm;
	self->count++;
}

hot static inline void
access_merge(Access* self, Access* access)
{
	for (auto i = 0; i < access->count; i++)
	{
		auto rel = access_at(access, i);
		access_add(self, rel->rel, rel->perm);
	}
}

hot static inline AccessRel*
access_find(Access* self, Str* name)
{
	for (auto i = 0; i < self->count; i++)
	{
		auto rel = access_at(self, i);
		if (str_compare(rel->rel->name, name))
			return rel;
	}
	return NULL;
}
