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
	ACCESS_RO_EXCLUSIVE
} AccessType;

struct AccessRecord
{
	Table*     table;
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
access_add(Access* self, Table* table, AccessType type)
{
	// find and upgrade access to the relation
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		if (record->table == table)
		{
			if (record->type < type)
				record->type = type;
			return;
		}
	}
	auto record = (AccessRecord*)buf_claim(&self->list, sizeof(AccessRecord));
	record->table = table;
	record->type  = type;
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
			if (record->table != record_with->table)
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
			default:
				abort();
			}
		}
	}
	return true;
}

static inline void
access_encode(Access* self, Buf* buf)
{
	encode_array(buf);
	for (auto i = 0; i < self->list_count; i++)
	{
		auto record = access_at(self, i);
		encode_array(buf);
		encode_target(buf, &record->table->config->schema, &record->table->config->name);
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
		default:
			abort();
		}
		encode_array_end(buf);
	}
	encode_array_end(buf);
}
