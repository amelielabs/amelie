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

typedef struct PartSnapshot PartSnapshot;

struct PartSnapshot
{
	Part*    part;
	uint64_t part_lsn;
	bool     active;
};

static inline void
part_snapshot_init(PartSnapshot* self, Part* part)
{
	self->part     = part;
	self->part_lsn = 0;
	self->active   = false;
}
