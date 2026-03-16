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
	int64_t   partitions;
	BranchMgr branches;
	VolumeMgr volumes;
};

static inline void
partitioning_init(Partitioning* self)
{
	self->partitions = 0;
	branch_mgr_init(&self->branches);
	volume_mgr_init(&self->volumes);
}

static inline void
partitioning_free(Partitioning* self)
{
	branch_mgr_free(&self->branches);
	volume_mgr_free(&self->volumes);
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
	branch_mgr_copy(&self->branches, &copy->branches);
	volume_mgr_copy(&self->volumes, &copy->volumes);
}

static inline void
partitioning_read(Partitioning* self, uint8_t** pos)
{
	uint8_t* pos_branches = NULL;
	uint8_t* pos_volumes  = NULL;
	Decode obj[] =
	{
		{ DECODE_INT,   "partitions", &self->partitions },
		{ DECODE_ARRAY, "branches",   &pos_branches     },
		{ DECODE_ARRAY, "volumes",    &pos_volumes      },
		{ 0,             NULL,         NULL             },
	};
	decode_obj(obj, "partitioning", pos);

	// branches
	branch_mgr_read(&self->branches, &pos_branches);

	// volumes
	volume_mgr_read(&self->volumes, &pos_volumes);
}

static inline void
partitioning_write(Partitioning* self, Buf* buf, int flags)
{
	encode_obj(buf);

	// partitions
	encode_raw(buf, "partitions", 10);
	encode_integer(buf, self->partitions);

	// branches
	encode_raw(buf, "branches", 8);
	branch_mgr_write(&self->branches, buf, flags);

	// volumes
	encode_raw(buf, "volumes", 7);
	volume_mgr_write(&self->volumes, buf, flags);

	encode_obj_end(buf);
}
