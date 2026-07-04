
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

static void
amelie_main(void* arg, int argc, char** argv)
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

enum
{
	AMELIE_OBJ      = 0x0323410L,
	AMELIE_OBJ_FREE = 0x0f0f0f0L
};

struct amelie
{
	int     type;
	Runtime runtime;
};

AMELIE_API amelie_t*
amelie_init(void)
{
	auto self = (amelie_t*)am_malloc(sizeof(amelie_t));
	self->type = AMELIE_OBJ;
	runtime_init(&self->runtime);
	return self;
}

AMELIE_API void
amelie_free(void* ptr)
{
	switch (*(int*)ptr) {
	case AMELIE_OBJ:
	{
		// all sessions must be closed at this point
		amelie_t* self = ptr;
		runtime_stop(&self->runtime);
		runtime_free(&self->runtime);
		break;
	}
	case AMELIE_OBJ_FREE:
	{
		fprintf(stderr, "\n%s(%p): attempt to use freed object\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	default:
	{
		fprintf(stderr, "\n%s(%p): unrecognized object type\n",
		        source_function, ptr);
		fflush(stderr);
		abort();
		break;
	}
	}
	*(int*)ptr = AMELIE_OBJ_FREE;
	am_free(ptr);
}

AMELIE_API int
amelie_open(amelie_t* self, const char* path, int argc, char** argv)
{
	// ensure amelie_open() was not called previously
	if (unlikely(runtime_started(&self->runtime)))
		return -1;
	auto status = runtime_start(&self->runtime, amelie_main, (void*)path,
	                            argc, argv);
	return status == RUNTIME_OK? 0: -1;
}
