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
	Buf list_ordered;
	int list_count;
	int count;
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
	self->count      = 0;
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
	self->count      = 0;
	buf_reset(&self->list);
}

static inline bool
access_empty(Access* self)
{
	return !self->list_count;
}

hot static inline void
access_add(Access* self, Relation* rel, LockId lock)
{
	// count total number of accesses, exclusive calls
	if (lock != LOCK_CALL)
		self->count++;

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
	buf_write(&self->list_ordered, &self->list_count, sizeof(int));
	self->list_count++;
}

hot static int
access_cmp(const void* p1, const void* p2, void* arg)
{
	Access* self = arg;
	auto a = access_at(self, *(int*)p1);
	auto b = access_at(self, *(int*)p2);
	return compare_uint64(a->rel->lock_order, b->rel->lock_order);
}

static inline void
access_sort(Access* self)
{
	qsort_r(self->list_ordered.start, self->list_count, sizeof(int),
	        access_cmp, self);
}

hot static inline void
access_merge(Access* self, Access* access)
{
	for (auto i = 0; i < access->list_count; i++)
	{
		auto record = access_at(access, i);
		access_add(self, record->rel, record->lock);
	}

	// order relations by lock_order
	access_sort(self);
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
