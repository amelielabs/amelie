
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
#include <amelie_cli_test.h>

void
cli_cmd_test(Cli* self, int argc, char** argv)
{
	unused(self);
	logger_set_cli(global()->logger, true, false);

	TestSuite suite;
	test_suite_init(&suite);
	str_set_cstr(&suite.option_result_dir, "_output");
	str_set_cstr(&suite.option_plan_file, "plan");

	int opt;
	while ((opt = getopt(argc + 1, argv - 1, "o:t:g:hf")) != -1) {
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
			error("usage: %s [-o output_dir] [-t test] [-g group] [-h] [-f]\n", argv[0]);
			break;
		}
	}

	opt_int_set(&config()->log_connections, false);
	error_catch (
		test_suite_run(&suite);
	);
	test_suite_cleanup(&suite);
	test_suite_free(&suite);
}
