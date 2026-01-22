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
	ID_NONE        = 0,
	ID             = 1 << 0,
	ID_INCOMPLETE  = 1 << 1
};

struct Id
{
	uint64_t id;
	uint64_t id_parent_begin;
	uint64_t id_parent_end;
	uint64_t id_part;
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
