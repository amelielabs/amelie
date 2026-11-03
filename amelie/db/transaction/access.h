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
	int  count_users;
	Buf  list;
	Buf  list_users;
	bool ro;
};

static inline AccessRel*
access_at(Access* self, int order)
{
	return &((AccessRel*)self->list.start)[order];
}

static inline Rel*
access_at_user(Access* self, int order)
{
	return ((Rel**)self->list_users.start)[order];
}

static inline void
access_init(Access* self)
{
	self->ro          = true;
	self->count       = 0;
	self->count_users = 0;
	buf_init(&self->list);
	buf_init(&self->list_users);
}

static inline void
access_free(Access* self)
{
	buf_free(&self->list);
	buf_free(&self->list_users);
}

static inline void
access_reset(Access* self)
{
	self->ro          = true;
	self->count       = 0;
	self->count_users = 0;
	buf_reset(&self->list);
	buf_reset(&self->list_users);
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

static inline bool
access_multi_user(Access* self)
{
	return self->count_users > 1;
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

	// add unique user
	for (auto i = 0; i < self->count_users; i++)
	{
		auto user = access_at_user(self, i);
		if (user == rel->parent)
			return;
	}

	buf_write(&self->list_users, &rel->parent, sizeof(Rel**));
	self->count_users++;
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
access_find(Access* self, Str* user, Str* name)
{
	for (auto i = 0; i < self->count; i++)
	{
		auto rel = access_at(self, i);
		if (str_compare(rel->rel->user, user) &&
		    str_compare(rel->rel->name, name))
			return rel;
	}
	return NULL;
}

hot static inline Rel*
access_find_user(Access* self, Str* name)
{
	for (auto i = 0; i < self->count_users; i++)
	{
		auto rel = access_at_user(self, i);
		if (str_compare(rel->name, name))
			return rel;
	}
	return NULL;
}
