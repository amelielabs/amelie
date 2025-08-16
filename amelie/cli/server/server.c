
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
#include <amelie_cli.h>

void
cli_cmd_init(Cli* self, int argc, char** argv)
{
	if (argc < 1)
		error("init <path> [options] expected");

	// amelie init path [server options]
	auto bootstrap = runtime_open(&self->runtime, argv[0], argc - 1, argv + 1);
	if (! bootstrap)
		error("directory already exists");

	// start, bootstrap and quick exit
	auto system = (System*)NULL;
	error_catch
	(
		system = system_create();
		system_start(system, bootstrap);

		// start pending coroutines before stop
		coroutine_sleep(0);
	);

	if (system)
	{
		system_stop(system);
		system_free(system);
	}
}

void
cli_cmd_start(Cli* self, int argc, char** argv)
{
	if (argc < 1)
		error("start <path> [options] expected");

	// amelie start path [server options]
	auto bootstrap = runtime_open(&self->runtime, argv[0], argc - 1, argv + 1);

	auto system = (System*)NULL;
	error_catch
	(
		system = system_create();
		system_start(system, bootstrap);

		// notify cli_start about start completion
		cond_signal(&self->task.status, CLI_RUN);

		// handle system requests
		system_main(system);
	);

	if (system)
	{
		system_stop(system);
		system_free(system);
	}
}

void
cli_cmd_stop(Cli* self, int argc, char** argv)
{
	// amelie stop path
	unused(self);	
	unused(argv);	
	if (argc < 1)
		error("stop <path> expected\n");

	// do nothing here
}

void
cli_cmd_backup(Cli* self, int argc, char** argv)
{
	if (argc < 1)
		error("backup <path> <remote> expected");

	Home home;
	home_init(&home);
	defer(home_free, &home);
	home_open(&home);

	// amelie backup path [remote options]
	auto bootstrap = runtime_create(&self->runtime, argv[0]);
	if (! bootstrap)
		error("directory already exists");

	// prepare remote
	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);
	login_mgr_set(&home.login_mgr, &remote, NULL, argc - 1, argv + 1);

	// disable log output
	if (str_is_cstr(remote_get(&remote, REMOTE_DEBUG), "0"))
		logger_set_to_stdout(&self->runtime.logger, false);

	restore(&remote);
}
