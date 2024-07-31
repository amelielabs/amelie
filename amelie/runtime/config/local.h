#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Local Local;

struct Local
{
	ConfigLocal config;
	Timezone*   timezone;
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
}

static inline void
local_free(Local* self)
{
	config_local_free(&self->config);
}
