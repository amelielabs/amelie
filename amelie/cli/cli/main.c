
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

int
main(int argc, char* argv[])
{
	Daemon daemon;
	daemon_init(&daemon);
	auto rc = daemon_main(&daemon, argc, argv);
	if (rc == -1)
		return EXIT_FAILURE;
	if (daemon.cmd == DAEMON_STOP)
		return EXIT_SUCCESS;

	Runtime runtime;
	runtime_init(&runtime);
	auto status = runtime_start(&runtime, cli_main, NULL, argc, argv);
	if (status == RUNTIME_OK)
		daemon_wait_for_signal();
	runtime_stop(&runtime);
	runtime_free(&runtime);
	return status == RUNTIME_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
