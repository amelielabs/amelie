
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
#include <amelie.h>
#include <amelie_main.h>
#include <amelie_main_slt.h>

void
cmd_test_slt(Main* self)
{
	main_advance(self, -2);
	opt_int_set(&config()->log_connections, false);

	Slt slt;
	slt_init(&slt);

	Str dir;
	str_set_cstr(&dir, "_output");
	Str file;
	str_init(&file);

	int opt;
	while ((opt = getopt(self->argc, self->argv, "o:t:h")) != -1) {
		switch (opt) {
		case 'o':
			str_set_cstr(&dir, optarg);
			break;
		case 't':
			str_set_cstr(&file, optarg);
			break;
		case 'h':
		default:
			error("usage: amelie slt [-t test] [-h]");
			break;
		}
	}
	if (str_empty(&file))
		error("test file is not set");

	error_catch (
		slt_start(&slt, &dir, &file);
	);
	slt_stop(&slt);
}
