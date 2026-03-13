
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
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_storage.h>
#include <amelie_object.h>
#include <amelie_heap.h>
#include <amelie_index.h>
#include <amelie_part.h>
#include <amelie_catalog.h>
#include <amelie_wal.h>
#include <amelie_service.h>

static void
syncer_main(void* arg)
{
	Syncer* self = arg;
	coroutine_set_name(am_self(), "syncer");
	coroutine_set_cancel_log(am_self(), false);
	uint64_t lsn_last = 0;
	for (;;)
	{
		coroutine_sleep(self->interval_us);
		auto lsn = state_lsn();
		if (lsn == lsn_last)
			continue;
		service_schedule(self->service, ACTION_SYNC, UINT64_MAX);
		lsn_last = lsn;
	}
}

void
syncer_init(Syncer* self, Service* service)
{
	self->interval_us  =  0;
	self->coroutine_id = -1;
	self->service      = service;
}

void
syncer_start(Syncer* self)
{
	if (! opt_int_of(&config()->wal_worker))
		return;

	// prepare periodic interval
	Interval interval;
	interval_init(&interval);
	interval_set(&interval, &config()->wal_sync_interval.string);
	if (interval.m > 0 || interval.d > 0)
		error("wal: sync interval cannot include day or month");

	// start periodic fsync notification
	self->interval_us = interval.us / 1000;
	if (self->interval_us != 0)
		self->coroutine_id = coroutine_create(syncer_main, self);
}

void
syncer_stop(Syncer* self)
{
	if (self->coroutine_id != -1)
	{
		coroutine_kill(self->coroutine_id);
		self->coroutine_id = -1;
		coroutine_sleep(0);
	}
}
