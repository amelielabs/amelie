
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

static void
system_describe(System* self, Buf* buf)
{
	unused(self);

	// secret
	buf_format(buf, "alter system set secret = {qstr};\n",
	           &state()->secret.string);

	// cdc
	auto cdc = opt_int_of(&state()->cdc);
	if (cdc != UINT64_MAX)
		buf_format(buf, "alter system set cdc = {u64};\n",
		           &state()->cdc.integer);
}

void
system_state_create(System* self, Buf* buf)
{
	// system state and settings
	system_describe(self, buf);

	// replication
	repl_describe(&self->repl, buf);
}

void
system_state_read(System* self)
{
	unused(self);

	// read file
	Separator sep;
	separator_init(&sep);
	defer(separator_free, &sep);
	file_import_stream(&sep.buf, "{s}/amelie.state", state_directory());

	// prepare eval
	auto eval = system_eval_allocate();
	defer(system_eval_free, eval);
	system_eval_create(eval);

	// parse and execute statements
	Str command;
	while (separator_read(&sep, &command))
	{
		system_eval(eval, &command);
		separator_advance(&sep);
	}
}

static void
system_state_write_to(System* self, char* path)
{
	auto buf = buf_create();
	defer_buf(buf);
	system_state_create(self, buf);

	// create file
	File file;
	file_init(&file);
	defer(file_close, &file);
	file_open_as(&file, path, O_CREAT|O_TRUNC|O_WRONLY, 0600);
	file_write_buf(&file, buf);

	// todo: sync
}

void
system_state_write(System* self)
{
	auto basedir = state_directory();

	char path[PATH_MAX];
	format(path, sizeof(path), "{s}/amelie.state.next", basedir);

	// write state
	system_state_write_to(self, path);

	if (! fs_exists("{s}/amelie.state", basedir))
	{
		fs_rename(path, "{s}/amelie.state", basedir);
		return;
	}

	// do atomic exchange
	fs_rename_exchange(path, "{s}/amelie.state", basedir);

	// remove previous file
	fs_unlink("{s}", path);
}
