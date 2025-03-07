
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

	Amelie amelie;
	amelie_init(&amelie);
	auto status = amelie_start(&amelie, argc, argv);
	if (status == AMELIE_RUN)
		daemon_wait_for_signal();
	amelie_stop(&amelie);
	amelie_free(&amelie);
	return status == AMELIE_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
