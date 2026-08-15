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

typedef struct Limits Limits;

enum
{
	// network
	LIMIT_SEND,

	// transaction
	LIMIT_WRITE,
	LIMIT_COMPUTE,

	// db
	LIMIT_MEMORY,

	// relations
	LIMIT_USERS,
	LIMIT_TABLES,
	LIMIT_INDEXES,
	LIMIT_CLONES,
	LIMIT_TOPICS,
	LIMIT_SUBSCRIPTIONS,
	LIMIT_FUNCTIONS,

	// options
	LIMIT_COLUMNS,
	LIMIT_KEYS,
	LIMIT_VALUES,
	LIMIT_ARGS,
	LIMIT_PARTITIONS,
	LIMIT_VECTOR,

	LIMIT_MAX
};

struct Limits
{
	uint64_t flags;
	int64_t  limits[LIMIT_MAX];
};

void limits_init(Limits*);
void limits_copy(Limits*, Limits*);
int  limits_find(Str*);
void limits_read(Limits*, uint8_t**);
void limits_write(Limits*, Buf*);

static inline bool
limits_is_set(Limits* self, int id)
{
	return (self->flags & (1 << id)) > 0;
}

static inline void
limits_set(Limits* self, int id, int64_t value)
{
	self->flags |= (1 << id);
	self->limits[id] = value;
}

static inline void
limits_unset(Limits* self, int id)
{
	self->flags &= ~(1 << id);
	self->limits[id] = 0;
}

static inline int64_t
limits_get(Limits* self, int id)
{
	return self->limits[id];
}

hot static inline bool
limits_check(Limits* self, int id, int64_t value)
{
	// check if limit is set
	if (!self || !(self->flags & (1 << id)))
		return true;
	return value <= self->limits[id];
}
