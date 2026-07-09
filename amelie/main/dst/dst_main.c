
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
#include <amelie_main_dst.h>

void
cmd_dst(Main* self)
{
	// amelie dst [options]
	Dst dst;
	dst_init(&dst);
	defer(dst_free, &dst);

	// parse options
	opts_set_argv(&dst.opts, self->argc, self->argv);

	logger_set_stdout(&runtime()->logger, true);
	logger_set_stdout_time(&runtime()->logger, false);
	logger_set_stdout_lf(&runtime()->logger, false);

	opt_int_set(&config()->log_connections, false);
	dst_run(&dst);
}
