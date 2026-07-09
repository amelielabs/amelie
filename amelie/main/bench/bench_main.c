
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
#include <amelie_main_bench.h>

void
cmd_bench(Main* self)
{
	// amelie bench name [options]
	opt_int_set(&config()->log_connections, false);

	Bench bench;
	bench_init(&bench, self);
	defer(bench_free, &bench);

	// parse command line
	opts_set_argv(&bench.opts, self->argc, self->argv);

	bench_run(&bench);
}
