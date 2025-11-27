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

typedef struct Local Local;

struct Local
{
	Timezone* timezone;
	uint64_t  time_us;
	Str       db;
	Str       format;
};

static inline void
local_init(Local* self)
{
	// derive default configuration
	self->timezone = runtime()->timezone;
	self->time_us  = 0;
	self->format   = config()->format.string;
	str_init(&self->db);
}

static inline void
local_free(Local* self)
{
	unused(self);
}

static inline void
local_reset(Local* self)
{
	// derive default configuration
	self->timezone = runtime()->timezone;
	self->format   = config()->format.string;
	str_init(&self->db);
}

static inline void
local_update_time(Local* self)
{
	self->time_us = time_us();
}

static inline void
local_encode_opt(Local* self, Buf* buf, Opt* opt)
{
	// replace timezone/format with local settings
	if (opt == &config()->timezone)
	{
		encode_string(buf, &self->timezone->name);
		return;
	}
	if (opt == &config()->format)
	{
		encode_string(buf, &self->format);
		return;
	}
	opt_encode(opt, buf);
}
