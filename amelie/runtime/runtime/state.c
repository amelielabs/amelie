
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

#include <amelie_base.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_rel.h>
#include <amelie_runtime.h>

void
state_init(State* self)
{
	memset(self, 0, sizeof(*self));
	opts_init(&self->opts);
}

void
state_free(State* self)
{
	opts_free(&self->opts);
}

void
state_prepare(State* self)
{
	OptsDef defs[] =
	{
		// system
		{ "directory",       OPT_STRING, OPT_E,             &self->directory,      NULL,           0           },
		{ "lsn",             OPT_INT,    OPT_E,             &self->lsn,            NULL,           0           },
		{ "rsn",             OPT_INT,    OPT_E|OPT_H,       &self->rsn,            NULL,           REL_MAX     },
		{ "checkpoint",      OPT_INT,    OPT_E,             &self->checkpoint,     NULL,           0           },
		{ "recover",         OPT_INT,    OPT_E|OPT_H,       &self->recover,        NULL,           0           },
		// persistent
		{ "cdc",             OPT_INT,    OPT_C|OPT_S|OPT_H, &self->cdc,            0,              UINT64_MAX  },
		{ "secret",          OPT_STRING, OPT_C|OPT_S|OPT_H, &self->secret,         0,              0           },
		{ "repl",            OPT_BOOL,   OPT_C,             &self->repl,           0,              false       },
		{ "repl_primary",    OPT_UUID,   OPT_C,             &self->repl_primary,   NULL,           0           },
		{  NULL,             0,          0,                  NULL,                 NULL,           0           },
	};
	opts_define(&self->opts, defs);
}
