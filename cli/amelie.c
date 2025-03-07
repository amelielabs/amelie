
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

#include <amelie_private.h>

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

	Main main;
	main_init(&main);
	auto status = main_start(&main, argc, argv);
	if (status == MAIN_RUN)
		daemon_wait_for_signal();
	main_stop(&main);
	main_free(&main);
	return status == MAIN_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
