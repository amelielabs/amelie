
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
#include <amelie_backend.h>

hot static void
pod_request(Pod* self, Ltr* ltr, Req* req)
{
	// create and add transaction to the prepared list (even on error)
	auto gtr = ltr->gtr;
	if (ltr->tr == NULL)
	{
		auto track = self->track;
		auto tr = tr_create(&track->cache);
		tr_set_id(tr, gtr->id);
		tr_set_limit(tr, &gtr->usage_write);
		tr_list_add(&track->prepared, tr);
		ltr->tr = tr;
	}

	// execute request
	vm_reset(&self->vm);
	reg_prepare(&self->vm.r, req->code->regs);

	Return ret;
	return_init(&ret);

	vm_run(&self->vm, gtr->local,
	        gtr,
	        ltr->tr,
	        NULL,
	        req->code,
	        req->code_data,
	        &req->arg,
	        (Value*)req->refs.start,
	        NULL,
	        &ret,
	        false,
	        req->start);

	if (ret.value)
		value_move(&req->result, ret.value);
}

hot static void
pod_run(Pod* self, Ltr* ltr)
{
	// set compute limit
	auto limits = ltr->gtr->local->limits;
	if (limits)
	{
		auto quota = &self->quota;
		if (limits_is_set(limits, LIMIT_COMPUTE))
			quota_set(quota, limits_get(limits, LIMIT_COMPUTE));
		else
			quota_reset(quota);
	}

	// execute incoming requests till close
	auto active = true;
	while (active)
	{
		auto msg = ltr_read(ltr);
		if (msg->id == MSG_LTR_STOP)
		{
			active = false;
			continue;
		}
		auto req = (Req*)msg;
		if (error_catch(pod_request(self, ltr, req)))
		{
			req->error = error_create(&am_self()->error);
			if (! ltr->error)
				ltr->error = req->error;
		}
		active = !req->dispatch->close;
		dispatch_complete(req->dispatch);
	}

	calls_reset(&self->vm.calls);
	ltr_complete(ltr);
}

void
pod_sync(Pod* self)
{
	// commit (or abort) pending transactions based on the global
	// commit state to resume streaming
	auto track = self->track;
	Consensus consensus;
	consensus_atomic_read(&track->consensus_atomic, &consensus);
	track_sync(track, &consensus);

	/*
	if (track_sync(track, &consensus))
		streaming_resume(self->part);
		*/
}

#if 0
static void
pod_retention(Pod* self)
{
	if (! tr_list_empty(&self->track->prepared))
		return;

	auto part = self->part;
	uint32_t id;
	if (likely(! storage_retention(&part->heap->storage, &id)))
		return;

	// cleanup vector stores
	flats_retention(&part->flats, id);

	// todo: cancel all matching streamers
	streaming_cancel(part);
}
#endif

static void
pod_main(void* arg)
{
	Pod* self  = arg;
	auto track = self->track;
	for (;;)
	{
#if 0
		Msg* msg;
		if (part->streams && !tr_list_empty(&track->prepared))
		{
			msg = track_read_time(track, 2);
			if (! msg)
			{
				pod_sync(self);
				continue;
			}
		} else {
			msg = track_read(track);
		}
#endif
		auto msg = track_read(track);

		switch (msg->id) {
		case MSG_LTR:
		{
			auto ltr = (Ltr*)msg;

			// abort and commit previously prepared transactions
			auto changed = track_sync(track, &ltr->consensus);

#if 0
			// run retention
			if (table->config->retention > 0)
				pod_retention(self);
#endif

			// execute transaction
			pod_run(self, ltr);

			(void)changed;
#if 0
			// resume streaming
			if (part->streams && changed)
				streaming_resume(self->part);
#endif
			break;
		}
#if 0
		case MSG_STREAM:
		{
			auto stream = (Stream*)msg;
			stream_next(stream);
			break;
		}
		case MSG_STREAM_CANCEL:
		{
			auto stream = container_of(msg, Stream, msg_cancel);
			stream_cancel(stream);
			break;
		}
#endif
		case MSG_STOP:
			// streaming_cancel(part);
			return;
		default:
			abort();
			break;
		}
	}
}

Pod*
pod_allocate(User* user)
{
	auto self = (Pod*)am_malloc(sizeof(Pod));
	self->user      =  user;
	self->track     = &user->track;
	self->worker_id = -1;
	quota_init(&self->quota);
	vm_init(&self->vm, &self->quota);
	list_init(&self->link);
	return self;
}

void
pod_free(Pod* self)
{
	vm_free(&self->vm);
	am_free(self);
}

void
pod_start(Pod* self, Task* task)
{
	if (self->worker_id != -1)
		return;
	track_set_backend(self->track, task);
	self->worker_id = coroutine_create(pod_main, self);
}

void
pod_stop(Pod* self)
{
	if (self->worker_id == -1)
		return;
	auto worker = coroutines_find(&am_task->coroutines, self->worker_id);
	assert(worker);
	Msg stop;
	msg_init(&stop, MSG_STOP);
	track_write(self->track, &stop);
	wait_event(&worker->on_exit, am_self());
	track_set_backend(self->track, NULL);
	self->worker_id = -1;
}
