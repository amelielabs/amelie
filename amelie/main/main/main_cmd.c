
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

#include <amelie>
#include <amelie_main.h>

static void
cmd_start(Main* self)
{
	// amelie start <path, bookmark> [options]

	// ensure path is set
	auto path = opt_string_of(&self->endpoint.path);
	if (str_empty(path))
		error("path is not defined");

	// start the database
	system_runtime_main(path->pos, self->argc, self->argv);
}

static void
cmd_stop(Main* self)
{
	// amelie stop <path, bookmark>

	// <path>/pid
	auto path = opt_string_of(&self->endpoint.path);
	auto buf = file_import("{str}/pid", path);
	defer_buf(buf);

	// read pid value
	Str pid_str;
	buf_str(buf, &pid_str);
	int64_t pid = -1;
	if (str_toint(&pid_str, &pid) == -1)
		error("invalid pid file");

	// signal
	kill(pid, SIGINT);
}

static void
cmd_backup(Main* self)
{
	// amelie backup <path, uri, bookmark> [path]

	// parse command line
	if (self->argc != 1)
		error("usage: amelie backup <path, uri, bookmark> <directory>");

	// disable log output
	if (! self->endpoint.debug.integer)
		logger_set_stdout(&runtime()->logger, false);

	// create backup
	opt_int_set(&config()->log_connections, false);
	restore(&self->endpoint, self->argv[0]);
}

static void
cmd_bookmark(Main* self)
{
	// amelie bookmark <name> options
	if (! self->argc)
		error("usage: amelie bookmark <name> [options]");

	// set bookmark name
	Str name;
	str_set_cstr(&name, self->argv[0]);
	main_advance(self, 1);

	// delete existing record first
	bookmarks_delete(&self->bookmarks, &name);
	if (! self->argc)
		return;

	// parse options
	main_configure(self);

	// ensure all options read
	if (self->argc)
		error("usage: amelie bookmark <name> [options]");

	// create new bookmark
	auto ref = bookmark_allocate();
	bookmarks_add(&self->bookmarks, ref);
	endpoint_copy(&ref->endpoint, &self->endpoint);
	opt_string_set(&ref->endpoint.name, &name);
}

extern void cmd_import(Main*);
extern void cmd_bench(Main*);
extern void cmd_test(Main*);

MainCmd
main_cmds[] =
{
	// server
	{ cmd_start,    false, true,  "start",    "Start database"                      },
	{ cmd_stop,     false, true,  "stop",     "Stop database"                       },
	{ cmd_backup,   false, true,  "backup",   "Create database backup"              },

	// client
	{ main_cli,     true,  true,  "cli",      "Open interactive console"            },
	{ cmd_import,   true,  true,  "import",   "Import data files into the database" },
	{ cmd_bookmark, true,  false, "bookmark", "Create, update or delete bookmark"   },
	{ cmd_bench,    true,  true,  "bench",    "Run benchmarks"                      },
	{ cmd_test,     false, false, "test",     "Run tests"                           },
	{ NULL,         false, false,  NULL,       NULL                                 },
};
