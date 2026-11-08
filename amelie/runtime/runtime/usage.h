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

hot static inline void
usage_update(Usage* self, int64_t size)
{
	self->usage += size;
	assert(self->usage >= 0);
}

hot static inline void
usage_add(Usage* self, int64_t size)
{
	if (likely(! size))
		return;
	usage_update(self, size);
	if (unlikely(self->limit_enable && (uint64_t)self->usage > self->limit))
		error("{s} limit reached", self->name);
}

hot static inline void
usage_add_reserve(Usage* self, int64_t size)
{
	if (likely(! size))
		return;
	uint64_t usage = self->usage + size;
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
	encode_int(buf, self->usage);

	// limit_enable
	encode_raw(buf, "limit_enabled", 13);
	encode_bool(buf, self->limit_enable);

	// limit
	encode_raw(buf, "limit", 5);
	encode_int(buf, self->limit);

	encode_obj_end(buf);
}
