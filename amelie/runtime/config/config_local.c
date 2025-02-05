
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>

void
config_local_init(ConfigLocal* self)
{
	memset(self, 0, sizeof(*self));
	vars_init(&self->vars);
}

void
config_local_free(ConfigLocal* self)
{
	vars_free(&self->vars);
}

void
config_local_prepare(ConfigLocal* self)
{
	VarDef defs[] =
	{
		{ "timezone", VAR_STRING, VAR_C|VAR_R|VAR_L, &self->timezone, NULL, 0 },
		{  NULL,      0,          0,                  NULL,           NULL, 0 }
	};
	vars_define(&self->vars, defs);
}
