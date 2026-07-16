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

typedef struct DstKey DstKey;

struct DstKey
{
	uint64_t key;
	union {
		int64_t value;
		float   value_vector[4];
	};
	Hashnode node;
};

static inline DstKey*
dst_key_allocate(uint64_t key)
{
	auto self = (DstKey*)am_malloc(sizeof(DstKey));
	memset(self, 0, sizeof(DstKey));
	self->key = key;
	return self;
}

static inline void
dst_key_free(DstKey* self)
{
	am_free(self);
}

static inline DstKey*
dst_key_copy(DstKey* self)
{
	auto copy = dst_key_allocate(0);
	memcpy(copy, self, sizeof(DstKey));
	return copy;
}
