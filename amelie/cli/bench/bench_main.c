
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
#include <amelie_cli_bench.h>

void
cli_cmd_bench(Cli* self, int argc, char** argv)
{
	// amelie bench name
	auto home = &self->home;
	home_open(home);
	opt_int_set(&config()->log_connections, false);

	Remote remote;
	remote_init(&remote);
	defer(remote_free, &remote);

	Bench bench;
	bench_init(&bench, &remote);
	defer(bench_free, &bench);

	// prepare remote
	error_catch
	(
		login_mgr_set(&self->home.login_mgr, &remote, &bench.opts, argc, argv);
		bench_run(&bench);
	);
}
