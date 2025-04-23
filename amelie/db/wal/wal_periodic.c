
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
wal_periodic_main(void* arg)
{
	WalPeriodic* self = arg;
	coroutine_set_name(am_self(), "wal_periodic");
	coroutine_set_cancel_log(am_self(), false);
	for (;;)
	{
		coroutine_sleep(self->interval_us);
		auto lsn = state_lsn();
		if (lsn == self->lsn)
			continue;
		wal_worker_request(self->worker, WAL_SYNC);
		self->lsn = lsn;
	}
}

void
wal_periodic_init(WalPeriodic* self, WalWorker* worker)
{
	self->interval_us  =  0;
	self->lsn          =  0;
	self->coroutine_id = -1;
	self->worker       = worker;
}

void
wal_periodic_start(WalPeriodic* self)
{
	if (! opt_int_of(&config()->wal_worker))
		return;

	// prepare periodic interval
	Interval interval;
	interval_init(&interval);
	interval_set(&interval, &config()->wal_sync_interval.string);
	if (interval.m > 0 || interval.d > 0)
		error("wal: sync interval cannot include day or month");

	// start periodic fsync worker
	self->interval_us = interval.us / 1000;
	if (self->interval_us != 0)
		self->coroutine_id = coroutine_create(wal_periodic_main, self);
}

void
wal_periodic_stop(WalPeriodic* self)
{
	if (self->coroutine_id != -1)
	{
		coroutine_kill(self->coroutine_id);
		self->coroutine_id = -1;
		coroutine_sleep(0);
	}
}
