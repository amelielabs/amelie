
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

#include <amelie_core.h>
#include <amelie.h>
#include <amelie_main.h>

static void
cmd_init(Main* self)
{
	// amelie init <path, bookmark> [options]
	main_open(self, MAIN_OPEN_LOCAL_NEW, NULL);
	defer(main_close, self);
}

static void
cmd_start(Main* self)
{
	// amelie start <path, bookmark> [options]

	// parse command line and start db
	main_open(self, MAIN_OPEN_LOCAL, NULL);
	defer(main_close, self);

	// notify start completion
	cond_signal(&am_task->status, RUNTIME_OK);

	// wait for stop
	task_recv();
}

static void
cmd_stop(Main* self)
{
	// amelie stop <path, bookmark>
	main_open(self, MAIN_OPEN_LOCAL_RUNNING, NULL);
	defer(main_close, self);

	// <path>/pid
	auto path = remote_get(&self->remote, REMOTE_PATH);
	auto buf = file_import("%s/pid", str_of(path));
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
	main_open(self, MAIN_OPEN_REMOTE, NULL);
	defer(main_close, self);
	if (self->argc != 1)
		error("usage: amelie backup <path, uri, bookmark> <directory>");

	// disable log output
	if (str_is_cstr(remote_get(&self->remote, REMOTE_DEBUG), "0"))
		logger_set_to_stdout(&runtime()->logger, false);

	// create backup
	restore(&self->remote, self->argv[0]);
}

static void
cmd_import(Main* self)
{
	// amelie import <path, uri, bookmark> <target> files ...
	Import import;
	import_init(&import, self);
	defer(import_free, &import);

	main_open(self, MAIN_OPEN_REMOTE, &import.opts);
	defer(main_close, self);

	opt_int_set(&config()->log_connections, false);
	import_run(&import);
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

	// load bookmarks only
	main_open(self, MAIN_OPEN_HOME, NULL);
	defer(main_close, self);

	// delete existing record first
	bookmark_mgr_delete(&self->bookmark_mgr, &name);
	if (! self->argc)
		return;

	// parse options
	main_configure(self, NULL);

	// create new bookmark
	auto ref = bookmark_allocate();
	bookmark_mgr_add(&self->bookmark_mgr, ref);
	remote_copy(&ref->remote, &self->remote);
	remote_set(&ref->remote, REMOTE_NAME, &name);
}

extern void cmd_bench(Main*);
extern void cmd_test(Main*);
extern void cmd_test_slt(Main*);

MainCmd
main_commands[] =
{
	{ cmd_init,     "init",     "Create database repository"          },
	{ cmd_start,    "start",    "Start database"                      },
	{ cmd_stop,     "stop",     "Stop database"                       },
	{ cmd_backup,   "backup",   "Create database backup"              },
	{ cmd_import,   "import",   "Import data files into the database" },
	{ cmd_bookmark, "bookmark", "Create, update or delete bookmark"   },
	{ cmd_bench,    "bench",    "Run benchmarks"                      },
	{ cmd_test,     "test",     "Run tests"                           },
	{ cmd_test_slt, "test_slt", "Run slt tests"                       },
	{ NULL,          NULL,       NULL                                 },
};
