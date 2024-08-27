
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_private.h>

int
main(int argc, char* argv[])
{
	Amelie amelie;
	amelie_init(&amelie);
	auto status = amelie_start(&amelie, argc, argv);
	if (status == AMELIE_RUN)
	{
		// wait signal for completion
		sigset_t mask;
		sigfillset(&mask);
		pthread_sigmask(SIG_BLOCK, &mask, NULL);
		sigemptyset(&mask);
		sigaddset(&mask, SIGINT);
		sigaddset(&mask, SIGTERM);
		int signo;
		sigwait(&mask, &signo);
	}
	amelie_stop(&amelie);
	amelie_free(&amelie);
	return status == AMELIE_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
