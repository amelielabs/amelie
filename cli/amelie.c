
//
// amelie.
//
// Real-Time SQL OLTP Database.
//
// Copyright (c) 2024 Dmitry Simonenko
// AGPL-3.0 Licensed.
//

#include <amelie_private.h>

int
main(int argc, char* argv[])
{
	Ctl ctl;
	ctl_init(&ctl);
	auto rc = ctl_main(&ctl, argc, argv);
	if (rc == -1)
		return EXIT_FAILURE;
	if (ctl.type == CTL_STOP)
		return EXIT_SUCCESS;

	Amelie amelie;
	amelie_init(&amelie);
	auto status = amelie_start(&amelie, argc, argv);
	if (status == AMELIE_RUN)
		ctl_wait_for_signal();
	amelie_stop(&amelie);
	amelie_free(&amelie);
	return status == AMELIE_ERROR? EXIT_FAILURE: EXIT_SUCCESS;
}
