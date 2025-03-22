
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>

static void
wal_worker_main(void* arg)
{
	auto self = (WalWorker*)arg;
	for (;;)
	{
		mutex_lock(&self->lock);
		while (! self->pending)
			cond_var_wait(&self->cond_var, &self->lock);
		int events = self->pending;
		self->pending = 0;
		mutex_unlock(&self->lock);

		if (events & WAL_CREATE)
			if (! wal_overflow(self->wal))
				continue;

		// process events
		error_catch
		(
			// sync current wal
			if (events & WAL_SYNC)
				wal_sync(self->wal, false);

			// create new wal
			if (events & WAL_CREATE)
				wal_create(self->wal, state_lsn() + 1);

			// sync new wal and the directory
			if (events & WAL_SYNC_CREATE)
				wal_sync(self->wal, true);
		);

		// stop
		if (events & WAL_SHUTDOWN)
			break;
	}
}

void
wal_worker_init(WalWorker* self, Wal* wal)
{
	self->pending = 0;
	self->wal     = wal;
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
	task_init(&self->task);
}

void
wal_worker_free(WalWorker* self)
{
	cond_var_free(&self->cond_var);
	mutex_free(&self->lock);
	task_free(&self->task);
}

void
wal_worker_start(WalWorker* self)
{
	task_create(&self->task, "wal", wal_worker_main, self);
}

void
wal_worker_stop(WalWorker* self)
{
	task_wait(&self->task);
}

void
wal_worker_request(WalWorker* self, int actions)
{
	mutex_lock(&self->lock);
	self->pending |= actions;
	cond_var_signal(&self->cond_var);
	mutex_unlock(&self->lock);
}
