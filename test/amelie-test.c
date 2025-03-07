
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
#include <amelie_test.h>

int
main(int argc, char* argv[])
{
	TestSuite suite;
	test_suite_init(&suite);
	suite.option_result_dir = "_output";
	suite.option_plan_file  = "plan";

	int opt;
	while ((opt = getopt(argc, argv, "o:t:g:hf")) != -1) {
		switch (opt) {
		case 'o':
			suite.option_result_dir = optarg;
			break;
		case 'f':
			suite.option_fix = 1;
			break;
		case 't':
			suite.option_test = optarg;
			break;
		case 'g':
			suite.option_group = optarg;
			break;
		case 'h':
		default:
			printf("usage: %s [-o output_dir] [-t test] [-g group] [-h] [-f]\n", argv[0]);
			return EXIT_SUCCESS;
		}
	}

	int rc;
	rc = test_suite_run(&suite);
	if (rc == -1)
		return EXIT_FAILURE;

	test_suite_cleanup(&suite);
	test_suite_free(&suite);
	return EXIT_SUCCESS;
}
