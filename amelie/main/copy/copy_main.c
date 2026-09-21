
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
#include <amelie_main_copy.h>

void
cmd_copy(Main* self)
{
	// amelie copy <path, uri, bookmark> files ...
	Copy copy;
	copy_init(&copy, self);
	defer(copy_free, &copy);

	logger_set_stdout(&runtime()->logger, true);
	logger_set_stdout_time(&runtime()->logger, false);
	logger_set_stdout_lf(&runtime()->logger, false);

	opt_int_set(&config()->log_connections, false);
	copy_run(&copy);
}
