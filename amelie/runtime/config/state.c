
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
	vars_init(&self->vars);
}

void
state_free(State* self)
{
	vars_free(&self->vars);
}

void
state_prepare(State* self)
{
	VarDef defs[] =
	{
		// state persistent
		{ "repl",                    VAR_BOOL,   VAR_C, &self->repl,         0,    false },
		{ "repl_primary",            VAR_STRING, VAR_C, &self->repl_primary, NULL, 0     },
		{ "replicas",                VAR_JSON,   VAR_C, &self->replicas,     NULL, 0     },
		{ "users",                   VAR_JSON,   VAR_C, &self->users,        NULL, 0     },
		// stats
		{ "connections",             VAR_INT,    VAR_E, &self->connections,  NULL, 0     },
		{ "sent_bytes",              VAR_INT,    VAR_E, &self->sent_bytes,   NULL, 0     },
		{ "recv_bytes",              VAR_INT,    VAR_E, &self->recv_bytes,   NULL, 0     },
		{ "writes",                  VAR_INT,    VAR_E, &self->writes,       NULL, 0     },
		{ "writes_bytes",            VAR_INT,    VAR_E, &self->writes_bytes, NULL, 0     },
		{ "ops",                     VAR_INT,    VAR_E, &self->ops,          NULL, 0     },
		{  NULL,                     0,          0,      NULL,               NULL, 0     },
	};
	vars_define(&self->vars, defs);
}

static void
state_save_to(State* self, const char* path)
{
	// get a list of variables
	auto buf = vars_list_persistent(&self->vars);
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
	Buf buf;
	buf_init(&buf);
	defer_buf(&buf);
	file_import(&buf, "%s", path);
	Str options;
	str_init(&options);
	buf_str(&buf, &options);
	vars_set(&self->vars, &options);
}
