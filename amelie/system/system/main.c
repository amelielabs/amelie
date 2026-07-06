
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

#include <amelie_runtime>
#include <amelie_server>
#include <amelie_db>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compiler>
#include <amelie_frontend.h>
#include <amelie_backend.h>
#include <amelie_session.h>
#include <amelie_system.h>

void
system_runtime_main(void* arg, int argc, char** argv)
{
	Repo repo;
	repo_init(&repo);
	defer(repo_close, &repo);

	System* system = NULL;
	auto on_error = error_catch
	(
		// create or open repository
		auto directory = arg;
		repo_open(&repo, directory, argc, argv);

		// create system object
		system = system_create();
		system_start(system, repo.bootstrap);

		// notify start completion
		cond_signal(&am_task->status, RUNTIME_OK);

		// handle system requests
		system_main(system);
	);
	if (system)
	{
		system_stop(system);
		system_free(system);
	}

	if (on_error)
		rethrow();
}
