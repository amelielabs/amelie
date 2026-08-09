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
	LIMIT_WRITE,
	LIMIT_CONNECTIONS,

	// db and runtime
	LIMIT_MEMORY,
	LIMIT_COMPUTE,

	// relations
	LIMIT_USERS,
	LIMIT_TABLES,
	LIMIT_INDEXES,
	LIMIT_CLONES,
	LIMIT_TOPICS,
	LIMIT_SUBSCRIPTIONS,
	LIMIT_FUNCTIONS,
	LIMIT_STATEMENTS,
	LIMIT_COLUMNS,
	LIMIT_COLUMNS_VECTOR,
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
void limits_read(Limits*, uint8_t**);
void limits_write(Limits*, Buf*);
int  limits_find(Str*);

hot static inline bool
limits_check(Limits* self, int category, int64_t value)
{
	// check if limit is set
	if (! (self->flags & (1 << category)))
		return true;
	return value <= self->limits[category];
}
