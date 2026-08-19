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

typedef struct Usage Usage;

struct Usage
{
	int64_t   usage;
	uint64_t  limit;
	bool      limit_enable;
	char*     name;
};

static inline void
usage_init(Usage* self, char* name)
{
	self->usage        = 0;
	self->limit        = 0;
	self->limit_enable = false;
	self->name         = name;
}

static inline void
usage_reset(Usage* self)
{
	self->usage        = 0;
	self->limit        = 0;
	self->limit_enable = false;
}

static inline void
usage_set_limit(Usage* self, bool limit_enable, uint64_t limit)
{
	self->limit        = limit;
	self->limit_enable = limit_enable;
}

hot static inline uint64_t
usage_sync(Usage* self, int64_t size)
{
	int64_t usage = __sync_fetch_and_add(&self->usage, size);
	usage += size;
	assert(usage >= 0);
	return usage;
}

hot static inline void
usage_update(Usage* self, int64_t size)
{
	if (likely(! size))
		return;
	usage_sync(self, size);
}

hot static inline void
usage_add(Usage* self, int64_t size)
{
	if (likely(! size))
		return;
	auto usage = usage_sync(self, size);
	if (unlikely(self->limit_enable && usage > self->limit))
		error("{s} limit reached", self->name);
}

hot static inline void
usage_add_reserve(Usage* self, int64_t size)
{
	if (likely(! size))
		return;
	auto usage = usage_sync(self, 0) + size;
	if (unlikely(self->limit_enable && usage > self->limit))
		error("{s} limit reached", self->name);
}

static inline void
usage_status(Usage* self, Buf* buf)
{
	// {}
	encode_obj(buf);

	// usage
	encode_raw(buf, "usage", 5);
	encode_int(buf, __sync_fetch_and_add(&self->usage, 0));

	// limit_enable
	encode_raw(buf, "limit_enabled", 13);
	encode_bool(buf, self->limit_enable);

	// limit
	encode_raw(buf, "limit", 5);
	encode_int(buf, self->limit);

	encode_obj_end(buf);
}
