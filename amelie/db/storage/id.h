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

typedef struct Id Id;

enum
{
	ID_NONE            = 0,
	ID_HEAP            = 1 << 0,
	ID_HEAP_INCOMPLETE = 1 << 1
};

struct Id
{
	uint64_t id;
	Uuid     id_tier;
} packed;

static inline void
id_init(Id* self)
{
	memset(self, 0, sizeof(*self));
}

static inline bool
id_compare(Id* self, Id* id)
{
	return !memcmp(self, id, sizeof(Id));
}

static inline int
id_of(const char* name, int64_t* id)
{
	// <id>.heap
	// <id>.heap.incomplete
	*id = 0;
	while (*name && *name != '.')
	{
		if (unlikely(! isdigit(*name)))
			return -1;
		if (unlikely(int64_mul_add_overflow(id, *id, 10, *name - '0')))
			return -1;
		name++;
	}

	int state = -1;
	if (! strcmp(name, ".heap.incomplete"))
		state = ID_HEAP_INCOMPLETE;
	else
	if (! strcmp(name, ".heap"))
		state = ID_HEAP;
	return state;
}
