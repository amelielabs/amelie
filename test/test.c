
//
// monotone
//
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone.h>
#include <monotone_test.h>

TestSuite suite;

int
main(int argc, char *argv[])
{
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
			return 0;
		}
	}

	int rc;
	rc = test_suite_run(&suite);
	if (rc == -1)
		return -1;

	test_suite_cleanup(&suite);
	test_suite_free(&suite);
	return 0;
}
