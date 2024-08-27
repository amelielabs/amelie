
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

int
main(int argc, char* argv[])
{
	Runner runner;
	runner_init(&runner);
	auto rc = runner_main(&runner, argc, argv);
	if (rc == -1)
		return EXIT_FAILURE;
	if (runner.type == RUNNER_STOP)
		return EXIT_SUCCESS;

	Amelie amelie;
	amelie_init(&amelie);
	auto status = amelie_start(&amelie, argc, argv);
	if (status == AMELIE_RUN)
		runner_wait_for_signal();
	amelie_stop(&amelie);
	amelie_free(&amelie);
	return status == AMELIE_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
