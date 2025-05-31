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
	ACCESS_RO,
	ACCESS_RW,
	ACCESS_RO_EXCLUSIVE,
	ACCESS_CALL
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
access_add(Access* self, Relation* rel, AccessType type)
{
	// find and upgrade access to the relation
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
	auto record = (AccessRecord*)buf_claim(&self->list, sizeof(AccessRecord));
	record->rel  = rel;
	record->type = type;
	self->list_count++;
}

hot static inline bool
access_try(Access* self, Access* with)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		for (auto j = 0; j < with->list_count; j++)
		{
			auto record_with = access_at(self, j);
			if (record->rel != record_with->rel)
				continue;
			switch (record->type) {
			case ACCESS_RO:
				// pass all
				break;
			case ACCESS_RW:
				// rw block only ro_exclusive
				if (record_with->type == ACCESS_RO_EXCLUSIVE)
					return false;
				break;
			case ACCESS_RO_EXCLUSIVE:
				// rw block only ro_exclusive
				if (record_with->type == ACCESS_RW)
					return false;
				break;
			case ACCESS_CALL:
				break;
			default:
				abort();
			}
		}
	}
	return true;
}

hot static inline AccessRecord*
access_find_schema(Access* self, Str* schema)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->schema, schema))
			return record;
	}
	return NULL;
}

hot static inline AccessRecord*
access_find(Access* self, Str* schema, Str* name)
{
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (str_compare(record->rel->schema, schema) &&
		    str_compare(record->rel->name, name))
			return record;
	}
	return NULL;
}

static inline void
access_encode(Access* self, Buf* buf)
{
	encode_array(buf);
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		encode_array(buf);
		encode_target(buf, record->rel->schema, record->rel->name);
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
		case ACCESS_CALL:
			encode_raw(buf, "call", 4);
			break;
		default:
			abort();
		}
		encode_array_end(buf);
	}
	encode_array_end(buf);
}
