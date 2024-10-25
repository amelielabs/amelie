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
	ConfigLocal config;
	Timezone*   timezone;
	uint64_t    time_us;
};

static inline void
local_init(Local* self, Global* global)
{
	config_local_init(&self->config);
	config_local_prepare(&self->config);

	// derive default timezone configuration
	self->config.timezone.flags = config()->timezone.flags;
	var_string_set(&self->config.timezone, &config()->timezone_default.string);
	self->timezone = global->timezone;
	self->time_us  = 0;
}

static inline void
local_free(Local* self)
{
	config_local_free(&self->config);
}

static inline void
local_update_time(Local* self)
{
	self->time_us = time_us();
}
