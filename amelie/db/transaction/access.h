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

typedef struct AccessRecord AccessRecord;
typedef struct Access       Access;

struct AccessRecord
{
	Rel*     rel;
	uint32_t perm;
};

struct Access
{
	Buf  list;
	int  list_count;
	bool ro;
	bool multi_user;
};

static inline AccessRecord*
access_at(Access* self, int order)
{
	return &((AccessRecord*)self->list.start)[order];
}

static inline void
access_init(Access* self)
{
	self->list_count = 0;
	self->ro         = true;
	self->multi_user = false;
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
	self->list_count = 0;
	self->ro         = true;
	self->multi_user = false;
	buf_reset(&self->list);
}

static inline bool
access_empty(Access* self)
{
	return !self->list_count;
}

static inline bool
access_ro(Access* self)
{
	return self->ro;
}

static inline bool
access_multi_user(Access* self)
{
	return self->multi_user;
}

hot static inline void
access_add(Access* self, Rel* rel, uint32_t perm)
{
	if ((perm & PERM_INSERT) ||
	    (perm & PERM_UPDATE) ||
	    (perm & PERM_DELETE))
	{
		self->ro = false;
	}
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->rel == rel)
		{
			record->perm |= perm;
			return;
		}
		if (! str_compare(record->rel->user, rel->user))
			self->multi_user = true;

	}
	auto record = (AccessRecord*)buf_emplace(&self->list, sizeof(AccessRecord));
	record->rel  = rel;
	record->perm = perm;
	self->list_count++;
}

hot static inline void
access_merge(Access* self, Access* access)
{
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		access_add(self, record->rel, record->perm);
	}
}

hot static inline AccessRecord*
access_find_user(Access* self, Str* user)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->user, user))
			return record;
	}
	return NULL;
}

hot static inline AccessRecord*
access_find(Access* self, Str* user, Str* name)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->user, user) &&
		    str_compare(record->rel->name, name))
			return record;
	}
	return NULL;
}
