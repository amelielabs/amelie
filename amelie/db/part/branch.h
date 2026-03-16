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

typedef struct Branch Branch;

struct Branch
{
	Str     name;
	int64_t id;
	int64_t snapshot;
	List    link;
};

static inline Branch*
branch_allocate(void)
{
	auto self = (Branch*)am_malloc(sizeof(Branch));
	self->id = 0;
	self->snapshot = 0;
	str_init(&self->name);
	list_init(&self->link);
	return self;
}

static inline void
branch_free(Branch* self)
{
	str_free(&self->name);
	am_free(self);
}

static inline void
branch_set_name(Branch* self, Str* name)
{
	str_free(&self->name);
	str_copy(&self->name, name);
}

static inline void
branch_set_id(Branch* self, uint64_t value)
{
	self->id = value;
}

static inline void
branch_set_snapshot(Branch* self, uint64_t value)
{
	self->snapshot = value;
}

static inline Branch*
branch_copy(Branch* from)
{
	auto self = branch_allocate();
	branch_set_name(self, &from->name);
	branch_set_id(self, from->id);
	branch_set_snapshot(self, from->snapshot);
	return self;
}

static inline Branch*
branch_read(uint8_t** pos)
{
	auto self = branch_allocate();
	errdefer(branch_free, self);
	Decode obj[] =
	{
		{ DECODE_STRING, "name",     &self->name     },
		{ DECODE_INT,    "id",       &self->id       },
		{ DECODE_INT,    "snapshot", &self->snapshot },
		{ 0,              NULL,       NULL           },
	};
	decode_obj(obj, "branch", pos);
	return self;
}

static inline void
branch_write(Branch* self, Buf* buf, int flags)
{
	unused(flags);

	// map
	encode_obj(buf);

	// name
	encode_raw(buf, "name", 4);
	encode_string(buf, &self->name);

	// id
	encode_raw(buf, "id", 2);
	encode_integer(buf, self->id);

	// snapshot
	encode_raw(buf, "snapshot", 8);
	encode_integer(buf, self->snapshot);

	encode_obj_end(buf);
}
