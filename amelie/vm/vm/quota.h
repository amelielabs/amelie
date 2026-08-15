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

typedef struct Quota Quota;

struct Quota
{
	uint64_t quota;
};

static inline void
quota_init(Quota* self)
{
	self->quota = UINT64_MAX;
}

static inline void
quota_reset(Quota* self)
{
	self->quota = UINT64_MAX;
}

static inline void
quota_set(Quota* self, uint64_t value)
{
	self->quota = value;
}

always_inline hot static inline void
quota_add(Quota* self)
{
	if (likely(self->quota > 0))
	{
		self->quota--;
		return;
	}
	error("compute limit reached");
}
