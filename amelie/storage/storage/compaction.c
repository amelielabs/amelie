
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
#include <amelie_volume>
#include <amelie_wal.h>
#include <amelie_engine.h>

hot static void
compaction_main(void* arg)
{
	Compaction* self = arg;
	for (;;)
	{
		if (engine_service(self->engine, &self->refresh, true))
			break;
	}
}

Compaction*
compaction_allocate(Engine* engine)
{
	auto self = (Compaction*)am_malloc(sizeof(Compaction));
	self->engine = engine;
	refresh_init(&self->refresh, engine);
	return self;
}

void
compaction_free(Compaction* self)
{
	refresh_free(&self->refresh);
	task_free(&self->task);
	am_free(self);
}

void
compaction_start(Compaction* self)
{
	task_create(&self->task, "compaction", compaction_main, self);
}

void
compaction_stop(Compaction* self)
{
	// send stop request
	if (task_active(&self->task))
		task_wait(&self->task);
}
