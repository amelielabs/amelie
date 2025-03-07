
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

#include <amelie.h>
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

	Cli cli;
	cli_init(&cli);
	auto status = cli_start(&cli, argc, argv);
	if (status == CLI_RUN)
		daemon_wait_for_signal();
	cli_stop(&cli);
	cli_free(&cli);
	return status == CLI_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
