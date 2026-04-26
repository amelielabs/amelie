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

typedef struct Snapshot Snapshot;

struct Snapshot
{
	int64_t   id;
	int64_t   id_parent;
	int64_t   snapshot;
	int64_t   snapshot_max;
	Snapshot* parent;
	Str       alias;
	List      link;
};

static inline void
snapshot_init(Snapshot* self)
{
	self->id           = 0;
	self->id_parent    = 0;
	self->snapshot     = 0;
	self->snapshot_max = 0;
	self->parent       = NULL;
	str_init(&self->alias);
	list_init(&self->link);
}

static inline void
snapshot_set_id(Snapshot* self, uint64_t value)
{
	self->id = value;
}

static inline void
snapshot_set_id_parent(Snapshot* self, uint64_t value)
{
	self->id_parent = value;
}

static inline void
snapshot_set_snapshot(Snapshot* self, uint64_t value)
{
	self->snapshot = value;
}

static inline void
snapshot_copy(Snapshot* self, Snapshot* from)
{
	snapshot_set_id(self, from->id);
	snapshot_set_id_parent(self, from->id_parent);
	snapshot_set_snapshot(self, from->snapshot);
}

static inline void
snapshot_read(Snapshot* self, uint8_t** pos)
{
	Decode obj[] =
	{
		{ DECODE_INT, "id",        &self->id        },
		{ DECODE_INT, "id_parent", &self->id_parent },
		{ DECODE_INT, "snapshot",  &self->snapshot  },
		{ 0,           NULL,        NULL            },
	};
	decode_obj(obj, "snapshot", pos);
}

static inline void
snapshot_write(Snapshot* self, Buf* buf, int flags)
{
	// map
	encode_obj(buf);

	// id
	encode_raw(buf, "id", 2);
	encode_int(buf, self->id);

	// id_parent
	encode_raw(buf, "id_parent", 9);
	encode_int(buf, self->id_parent);

	// snapshot
	encode_raw(buf, "snapshot", 8);
	encode_int(buf, self->snapshot);

	// snapshot_max
	if (flags_has(flags, FMETRICS))
	{
		encode_raw(buf, "snapshot_max", 12);
		encode_int(buf, self->snapshot_max);
	}

	encode_obj_end(buf);
}
