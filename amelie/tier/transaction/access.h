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
	Relation* rel;
	LockId    lock;
};

struct Access
{
	Buf list;
	int list_count;
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
	buf_reset(&self->list);
}

hot static inline void
access_add(Access* self, Relation* rel, LockId lock)
{
	// find and upgrade access to the relation or catalog
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->rel == rel)
		{
			if (record->lock < lock)
				record->lock = lock;
			return;
		}
	}
	auto record = (AccessRecord*)buf_emplace(&self->list, sizeof(AccessRecord));
	record->rel  = rel;
	record->lock = lock;
	self->list_count++;
}

hot static inline void
access_merge(Access* self, Access* access)
{
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		access_add(self, record->rel, record->lock);
	}
}

hot static inline AccessRecord*
access_find_db(Access* self, Str* db)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->db, db))
			return record;
	}
	return NULL;
}

hot static inline AccessRecord*
access_find(Access* self, Str* db, Str* name)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->db, db) &&
		    str_compare(record->rel->name, name))
			return record;
	}
	return NULL;
}

hot static inline int
access_tables(Access* self)
{
	auto count = 0;
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->lock == LOCK_CALL)
			continue;
		count++;
	}
	return count;
}

static inline bool
access_encode(Access* self, Buf* buf)
{
	auto call = false;
	encode_array(buf);
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		encode_array(buf);
		encode_string(buf, record->rel->name);
		lock_id_encode(buf, record->lock);
		if (record->lock == LOCK_CALL)
			call = true;
		encode_array_end(buf);
	}
	encode_array_end(buf);
	return call;
}
