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
}
