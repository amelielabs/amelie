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
	Str*      format;
};

static inline void
local_init(Local* self, Global* global)
{
	// derive default configuration
	self->timezone = global->timezone;
	self->time_us  = 0;
	self->format   = &config()->format.string;
}

static inline void
local_free(Local* self)
{
	unused(self);
}

static inline void
local_update_time(Local* self)
{
	self->time_us = time_us();
}
