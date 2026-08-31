
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
#include <amelie_main_import.h>

void
cmd_import(Main* self)
{
	// amelie import <path, uri, bookmark> files ...
	Import import;
	import_init(&import, self);
	defer(import_free, &import);

	logger_set_stdout(&runtime()->logger, true);
	logger_set_stdout_time(&runtime()->logger, false);
	logger_set_stdout_lf(&runtime()->logger, false);

	opt_int_set(&config()->log_connections, false);
	import_run(&import);
}
