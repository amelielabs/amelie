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
	Str       user;
	Random    random;
};

static inline void
local_init(Local* self)
{
	// derive default configuration
	self->timezone = runtime()->timezone;
	self->time_us  = 0;
	random_init(&self->random);
	str_init(&self->user);
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
	str_init(&self->user);
}

static inline void
local_encode_opt(Local* self, Buf* buf, Opt* opt)
{
	// replace timezone with local settings
	if (opt == &config()->timezone)
	{
		encode_str(buf, &self->timezone->name);
		return;
	}
	opt_encode(opt, buf);
}
