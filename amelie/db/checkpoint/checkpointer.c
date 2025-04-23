
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

static void
checkpointer_run(Rpc* rpc, void* arg)
{
	Checkpointer* self = arg;

	// prepare checkpoint
	uint64_t lsn = state_lsn();
	if (lsn == state_checkpoint())
		return;

	// take exclusive system lock
	control_lock();
	auto locked = true;

	Checkpoint cp;
	checkpoint_init(&cp, self->mgr);

	auto on_error = error_catch
	(
		// prepare checkpoint
		int workers = rpc_arg(rpc, 0);
		checkpoint_begin(&cp, lsn, workers);

		// run workers and create snapshots
		checkpoint_run(&cp);

		// unlock system
		control_unlock();
		locked = false;

		// wait for completion
		checkpoint_wait(&cp);
	);

	checkpoint_free(&cp);

	if (on_error)
	{
		if (locked)
			control_unlock();
		rethrow();
	}

	// run db cleanup
	self->mgr->iface->complete(self->mgr->iface_arg);
}

static void
checkpointer_main(void* arg)
{
	Checkpointer* self = arg;
	coroutine_set_name(am_self(), "checkpoint");
	coroutine_set_cancel_log(am_self(), false);
	for (;;)
	{
		auto rpc = rpc_queue_pop(&self->req_queue);
		if (! rpc)
		{
			event_wait(&self->req_queue_event, -1);
			continue;
		}
		rpc_execute(rpc, checkpointer_run, self);
	}
}

static void
checkpointer_periodic_main(void* arg)
{
	Checkpointer* self = arg;
	coroutine_set_name(am_self(), "checkpointer_periodic");
	coroutine_set_cancel_log(am_self(), false);
	for (;;)
	{
		int workers = opt_int_of(&config()->checkpoint_workers);
		if (workers == 0)
			workers = 1;
		coroutine_sleep(self->interval_us);
		rpc(global()->control->system, RPC_CHECKPOINT, 1, workers);
	}
}

void
checkpointer_init(Checkpointer* self, CheckpointMgr* mgr)
{
	self->mgr = mgr;
	self->worker_id = -1;
	self->worker_periodic_id = -1;
	self->interval_us = 0;
	event_init(&self->req_queue_event);
	rpc_queue_init(&self->req_queue);
}

void
checkpointer_start(Checkpointer* self)
{
	// prepare periodic interval
	Interval interval;
	interval_init(&interval);
	interval_set(&interval, &config()->checkpoint_interval.string);
	if (interval.m > 0 || interval.d > 0)
		error("checkpoint: interval cannot include day or month");

	// start periodic checkpoint worker
	self->interval_us = interval.us / 1000;
	if (self->interval_us != 0)
		self->worker_periodic_id = coroutine_create(checkpointer_periodic_main, self);
	else
		info("checkpoint: periodic checkpoint is disabled");

	// start checkpoint worker
	self->worker_id = coroutine_create(checkpointer_main, self);
}

void
checkpointer_stop(Checkpointer* self)
{
	if (self->worker_periodic_id != -1)
	{
		coroutine_kill(self->worker_periodic_id);
		self->worker_periodic_id = -1;
	}

	if (self->worker_id != -1)
	{
		coroutine_kill(self->worker_id);
		self->worker_id = -1;
	}
}

void
checkpointer_request(Checkpointer* self, Rpc* rpc)
{
	rpc_queue_add(&self->req_queue, rpc);
	event_signal(&self->req_queue_event);
}
