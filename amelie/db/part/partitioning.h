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

typedef struct Partitioning Partitioning;

struct Partitioning
{
	int64_t partitions;
};

static inline void
partitioning_init(Partitioning* self)
{
	self->partitions = 0;
}

static inline void
partitioning_free(Partitioning* self)
{
	unused(self);
}

static inline void
partitioning_set_partitions(Partitioning* self, int value)
{
	self->partitions = value;
}

static inline void
partitioning_copy(Partitioning* self, Partitioning* copy)
{
	partitioning_set_partitions(copy, self->partitions);
}

static inline void
partitioning_read(Partitioning* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_INT, "partitions", &self->partitions },
		{ 0,           NULL,         NULL             },
	};
	decode_obj(obj, "partitioning", pos);
}

static inline void
partitioning_write(Partitioning* self, Buf* buf, int flags)
{
	unused(flags);
	encode_obj(buf);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_int(buf, self->partitions);

	encode_obj_end(buf);
}
