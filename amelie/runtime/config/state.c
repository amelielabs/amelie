
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
		{ "version",       OPT_STRING, OPT_E, &self->version,      "0.4.0", 0     },
		{ "directory",     OPT_STRING, OPT_E, &self->directory,     NULL,   0     },
		{ "checkpoint",    OPT_INT,    OPT_E, &self->checkpoint,    NULL,   0     },
		{ "lsn",           OPT_INT,    OPT_E, &self->lsn,           NULL,   0     },
		{ "psn",           OPT_INT,    OPT_E, &self->psn,           NULL,   0     },
		{ "read_only",     OPT_BOOL,   OPT_E, &self->read_only,     NULL,   false },
		// persistent
		{ "repl",          OPT_BOOL,   OPT_C, &self->repl,          0,      false },
		{ "repl_primary",  OPT_STRING, OPT_C, &self->repl_primary,  NULL,   0     },
		{ "replicas",      OPT_JSON,   OPT_C, &self->replicas,      NULL,   0     },
		{ "users",         OPT_JSON,   OPT_C, &self->users,         NULL,   0     },
		// stats
		{ "connections",   OPT_INT,    OPT_E, &self->connections,   NULL,   0     },
		{ "sent_bytes",    OPT_INT,    OPT_E, &self->sent_bytes,    NULL,   0     },
		{ "recv_bytes",    OPT_INT,    OPT_E, &self->recv_bytes,    NULL,   0     },
		{ "writes",        OPT_INT,    OPT_E, &self->writes,        NULL,   0     },
		{ "writes_bytes",  OPT_INT,    OPT_E, &self->writes_bytes,  NULL,   0     },
		{ "ops",           OPT_INT,    OPT_E, &self->ops,           NULL,   0     },
		{  NULL,           0,          0,      NULL,                NULL,   0     },
	};
	opts_define(&self->opts, defs);
}

static void
state_save_to(State* self, const char* path)
{
	// get a list of optiables
	auto buf = opts_list_persistent(&self->opts);
	defer_buf(buf);

	// convert to json
	Buf text;
	buf_init(&text);
	defer_buf(&text);
	uint8_t* pos = buf->start;
	json_export_pretty(&text, NULL, &pos);

	// create state file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_RDWR, 0600);
	file_write_buf(&file, &text);
}

void
state_save(State* self, const char* path)
{
	if (! fs_exists("%s", path))
	{
		state_save_to(self, path);
		return;
	}

	// remove old saved state, if exists
	if (fs_exists("%s.old", path))
		fs_unlink("%s.old", path);

	// save existing state as a old
	fs_rename(path, "%s.old", path);

	// create state file
	state_save_to(self, path);

	// remove old state file
	fs_unlink("%s.old", path);
}

void
state_open(State* self, const char* path)
{
	auto buf = file_import("%s", path);
	defer_buf(buf);
	Str options;
	str_init(&options);
	buf_str(buf, &options);
	opts_set(&self->opts, &options);
}
