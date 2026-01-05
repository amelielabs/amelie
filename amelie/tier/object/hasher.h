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

typedef struct HasherGroup HasherGroup;
typedef struct Hasher      Hasher;

struct HasherGroup
{
	uint16_t hash_min;
	uint16_t hash_max;
	int      size;
	int      count;
	int      pos;
};

struct Hasher
{
	uint16_t hash_min;
	uint16_t hash_max;
	Buf      parts;
	Buf      groups;
	int      groups_count;
	Keys*    keys;
};

static inline void
hasher_init(Hasher* self)
{
	self->keys         = NULL;
	self->hash_min     = UINT16_MAX;
	self->hash_max     = 0;
	self->groups_count = 0;
	buf_init(&self->parts);
	buf_init(&self->groups);
}

static inline void
hasher_free(Hasher* self)
{
	buf_free(&self->parts);
	buf_free(&self->groups);
}

static inline void
hasher_reset(Hasher* self)
{
	self->keys         = NULL;
	self->hash_min     = UINT16_MAX;
	self->hash_max     = 0;
	self->groups_count = 0;
	buf_reset(&self->groups);
	if (! buf_empty(&self->parts))
		memset(buf_cstr(&self->parts), 0, sizeof(uint32_t) * UINT16_MAX);
}

static inline void
hasher_prepare(Hasher* self, Keys* keys)
{
	self->keys = keys;
	buf_reserve(&self->parts, sizeof(uint32_t) * UINT16_MAX);
}

hot static inline void
hasher_add(Hasher* self, Row* row)
{
	// get hash partition
	auto hash_partition = row_hash(row, self->keys) % UINT16_MAX;

	// track min/max
	if (hash_partition < self->hash_min)
		self->hash_min = hash_partition;
	if (hash_partition > self->hash_max)
		self->hash_max = hash_partition;

	// sum all rows per hash partition
	buf_u32(&self->parts)[ hash_partition ] += row_size(row);
}

hot static inline void
hasher_add_group(Hasher* self, HasherGroup* group)
{
	group->pos = self->groups_count;
	buf_write(&self->groups, group, sizeof(HasherGroup));
	self->groups_count++;
}

static inline void
hasher_group_init(HasherGroup* self)
{
	self->hash_min = UINT16_MAX;
	self->hash_max = 0;
	self->size     = 0;
	self->count    = 0;
	self->pos      = 0;
}

static inline bool
hasher_group_by_add(Hasher* self, HasherGroup* group, int at, int limit)
{
	if (unlikely(! group->count))
		self->hash_min = at;
	if (at > group->hash_max)
		group->hash_max = at;
	group->size += buf_u32(&self->parts)[at];
	group->count++;
	return group->size > limit;
}

hot static inline void
hasher_group_by(Hasher* self, int size)
{
	// group hash partitions by size
	HasherGroup group;
	hasher_group_init(&group);
	for (auto at = 0; at < UINT16_MAX; at++)
	{
		if (! hasher_group_by_add(self, &group, at, size))
			continue;
		hasher_add_group(self, &group);
		hasher_group_init(&group);
	}
	if (group.count > 0)
		hasher_add_group(self, &group);
}

hot static inline HasherGroup*
hasher_match(Hasher* self, Row* row)
{
	// get hash partition
	auto hash_partition = row_hash(row, self->keys) % UINT16_MAX;

	// find matching group
	auto groups = (HasherGroup*)self->groups.start;
	for (auto at = 0; at < self->groups_count; at++)
	{
		auto group = &groups[at];
		auto in = group->hash_min <= hash_partition && hash_partition <= group->hash_max;
		if (in)
			return group;
	}
	return NULL;
}
