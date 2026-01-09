
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
#include <amelie_tier>
#include <amelie_catalog.h>
#include <amelie_lock.h>
#include <amelie_wal.h>
#include <amelie_storage.h>

hot static void
compaction_main(void* arg)
{
	Compaction* self = arg;
	Refresh refresh;
	refresh_init(&refresh, self->storage);
	defer(refresh_free, &refresh);
	for (;;)
	{
		if (storage_service(self->storage, &refresh, true))
			break;
	}
}

Compaction*
compaction_allocate(Storage* storage)
{
	auto self = (Compaction*)am_malloc(sizeof(Compaction));
	self->storage = storage;
	return self;
}

void
compaction_free(Compaction* self)
{
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
