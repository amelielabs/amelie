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

typedef enum
{
	ACCESS_UNDEF,
	// relations
	ACCESS_RO,
	ACCESS_RW,
	ACCESS_RO_EXCLUSIVE,
	ACCESS_EXCLUSIVE,
	ACCESS_CALL,
	// catalog
	ACCESS_CATALOG,
	ACCESS_CATALOG_EXCLUSIVE
} AccessType;

struct AccessRecord
{
	Relation*  rel;
	AccessType type;
};

struct Access
{
	Buf list;
	int list_count;
	int tables;
};

static inline AccessRecord*
access_at(Access* self, int order)
{
	return &((AccessRecord*)self->list.start)[order];
}

static inline void
access_init(Access* self)
{
	self->tables = 0;
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
	self->tables = 0;
	self->list_count = 0;
	buf_reset(&self->list);
}

hot static inline void
access_add(Access* self, Relation* rel, AccessType type)
{
	// keep the total non unique number of accesses to tables
	if (rel && type != ACCESS_CALL)
		self->tables++;

	// find and upgrade access to the relation or catalog
	//
	// catalog relation is null
	//
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->rel == rel)
		{
			if (record->type < type)
				record->type = type;
			return;
		}
	}
	auto record = (AccessRecord*)buf_emplace(&self->list, sizeof(AccessRecord));
	record->rel  = rel;
	record->type = type;
	self->list_count++;
}

hot static inline void
access_merge(Access* self, Access* with)
{
	for (auto i = 0; i < with->list_count; i++)
	{
		auto record = access_at(with, i);
		access_add(self, record->rel, record->type);
	}
}

hot static inline bool
access_try(Access* self, Access* access)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto current = access_at(self, i);
		for (auto j = 0; j < access->list_count; j++)
		{
			auto attempt = access_at(self, j);
			// catalog relation is null
			if (current->rel != attempt->rel)
				continue;
			switch (current->type) {
			case ACCESS_RO:
				if (attempt->type == ACCESS_EXCLUSIVE)
					return false;
				break;
			case ACCESS_RW:
				if (attempt->type == ACCESS_RO_EXCLUSIVE ||
				    attempt->type == ACCESS_EXCLUSIVE)
					return false;
				break;
			case ACCESS_RO_EXCLUSIVE:
				if (attempt->type == ACCESS_RW ||
				    attempt->type == ACCESS_EXCLUSIVE)
					return false;
				break;
			case ACCESS_EXCLUSIVE:
				return false;
			case ACCESS_CALL:
				if (attempt->type == ACCESS_EXCLUSIVE)
					return false;
				break;
			case ACCESS_CATALOG:
				if (attempt->type == ACCESS_CATALOG_EXCLUSIVE)
					return false;
				break;
			case ACCESS_CATALOG_EXCLUSIVE:
				return false;
			default:
				abort();
			}
		}
	}
	// pass
	return true;
}

hot static inline AccessRecord*
access_find_db(Access* self, Str* db)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (! record->rel)
			continue;
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
		if (! record->rel)
			continue;
		if (str_compare(record->rel->db, db) &&
		    str_compare(record->rel->name, name))
			return record;
	}
	return NULL;
}

hot static inline bool
access_has_targets(Access* self)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (! record->rel)
			continue;
		if (record->type == ACCESS_CALL)
			continue;
		return true;
	}
	return false;
}

static inline bool
access_encode(Access* self, Buf* buf)
{
	auto has_call = false;
	encode_array(buf);
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		encode_array(buf);
		encode_string(buf, record->rel->name);
		switch (record->type) {
		case ACCESS_RO:
			encode_raw(buf, "ro", 2);
			break;
		case ACCESS_RW:
			encode_raw(buf, "rw", 2);
			break;
		case ACCESS_RO_EXCLUSIVE:
			encode_raw(buf, "ro_exclusive", 12);
			break;
		case ACCESS_EXCLUSIVE:
			encode_raw(buf, "exclusive", 9);
			break;
		case ACCESS_CALL:
			encode_raw(buf, "call", 4);
			has_call = true;
			break;
		case ACCESS_CATALOG:
			encode_raw(buf, "catalog", 7);
			break;
		case ACCESS_CATALOG_EXCLUSIVE:
			encode_raw(buf, "catalog_exclusive", 17);
			break;
		default:
			abort();
		}
		encode_array_end(buf);
	}
	encode_array_end(buf);
	return has_call;
}
