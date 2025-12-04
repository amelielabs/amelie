
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
#include <amelie_main_test.h>

void
cmd_test(Main* self)
{
	main_advance(self, -2);
	logger_set_cli(&runtime()->logger, true, false);

	TestSuite suite;
	test_suite_init(&suite);
	defer(test_suite_free, &suite);

	str_set_cstr(&suite.option_result_dir, "_output");
	str_set_cstr(&suite.option_plan_file, "plan");

	int opt;
	while ((opt = getopt(self->argc, self->argv, "o:t:g:hf")) != -1) {
		switch (opt) {
		case 'o':
			str_set_cstr(&suite.option_result_dir, optarg);
			break;
		case 'f':
			suite.option_fix = true;
			break;
		case 't':
			str_set_cstr(&suite.option_test, optarg);
			break;
		case 'g':
			str_set_cstr(&suite.option_group, optarg);
			break;
		case 'h':
		default:
			error("usage: amelie test [-o output_dir] [-t test] [-g group] [-h] [-f]\n");
			break;
		}
	}

	opt_int_set(&config()->log_connections, false);
	error_catch (
		test_suite_run(&suite);
	);
	test_suite_cleanup(&suite);
}
