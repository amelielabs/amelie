
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>

static void
checkpointer_run(Rpc* rpc, void* arg)
{
	Checkpointer* self = arg;

	// prepare checkpoint
	uint64_t lsn = config_lsn();
	if (lsn == config_checkpoint())
		return;

	// take exclusive system lock
	control_lock();
	auto locked = true;

	Checkpoint cp;
	checkpoint_init(&cp, self->mgr);

	Exception e;
	if (enter(&e))
	{
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
	}

	checkpoint_free(&cp);
	if (leave(&e))
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
	coroutine_set_name(am_self(), "checkpointer");
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

void
checkpointer_init(Checkpointer* self, CheckpointMgr* mgr)
{
	self->mgr = mgr;
	self->coroutine_id = -1;
	event_init(&self->req_queue_event);
	rpc_queue_init(&self->req_queue);
}

void
checkpointer_start(Checkpointer* self)
{
	self->coroutine_id = coroutine_create(checkpointer_main, self);
}

void
checkpointer_stop(Checkpointer* self)
{
	if (self->coroutine_id != -1)
	{
		coroutine_kill(self->coroutine_id);
		self->coroutine_id = -1;
	}
}

void
checkpointer_request(Checkpointer* self, Rpc* rpc)
{
	rpc_queue_add(&self->req_queue, rpc);
	event_signal(&self->req_queue_event);
}
