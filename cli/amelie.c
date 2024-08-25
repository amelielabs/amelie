
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
	auto rc = amelie_start(&amelie, argc, argv);
	if (rc == AMELIE_RUN) {
		while (getchar() != EOF);
	}
	amelie_stop(&amelie);
	amelie_free(&amelie);
	return rc == AMELIE_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
