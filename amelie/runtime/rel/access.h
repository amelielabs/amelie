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
	Buf list;
	Buf list_ordered;
	int list_count;
};

static inline AccessRecord*
access_at(Access* self, int order)
{
	return &((AccessRecord*)self->list.start)[order];
}

static inline AccessRecord*
access_at_ordered(Access* self, int order)
{
	auto ref = ((int*)self->list_ordered.start)[order];
	return access_at(self, ref);
}

static inline void
access_init(Access* self)
{
	self->list_count = 0;
	buf_init(&self->list);
	buf_init(&self->list_ordered);
}

static inline void
access_free(Access* self)
{
	buf_free(&self->list);
	buf_free(&self->list_ordered);
}

static inline void
access_reset(Access* self)
{
	self->list_count = 0;
	buf_reset(&self->list);
	buf_reset(&self->list_ordered);
}

static inline bool
access_empty(Access* self)
{
	return !self->list_count;
}

hot static inline void
access_add(Access* self, Rel* rel, uint32_t perm)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->rel == rel)
		{
			record->perm |= perm;
			return;
		}
	}
	auto record = (AccessRecord*)buf_emplace(&self->list, sizeof(AccessRecord));
	record->rel  = rel;
	record->perm = perm;
	buf_write(&self->list_ordered, &self->list_count, sizeof(int));
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
