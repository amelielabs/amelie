
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
#include <amelie_heap>
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_compute.h>

static void
apply_record(Apply* self, Db* db, Action* action)
{
	auto compute = self->compute;
	auto iface   = self->compute->iface;
	if (! self->session)
		self->session = iface->session_create(compute, compute->iface_arg);

	auto req      = &self->req;
	auto endpoint = &req->endpoint;

	// todo: output to none
	buf_reset(&self->buf);
	output_set_buf(&req->output, &self->buf);
	output_set(&req->output, endpoint, &output_json, NULL);

	// restore request
	auto record = (Record*)action->data;
	write_read(record_data(record),
	           &endpoint->db_id,
	           &endpoint->timezone,
	           &endpoint->time,
	           &endpoint->seed,
	           &endpoint->rel,
	           &endpoint->content_type,
	           &endpoint->content);

	// assign db
	request_set(req, db, false);
	req->replay = true;

	// execute
	compute->iface->session_execute(self->session, req);
}

static void
apply_checkpoint(Apply* self, Db* db, Action* action)
{
	unused(self);
	storage_checkpoint_db(share()->storage, db);
	uint64_t* lsn = action->complete->arg;
	*lsn = db->checkpoints.current->id;
}

static void
apply_main(void* arg)
{
	Apply* self = arg;

	// main
	auto catalog = &share()->storage->catalog;
	auto service = &share()->storage->service;
	auto sub     = &self->sub;
	auto req     = &self->req;
	for (;;)
	{
		auto action = service_next(service, sub);
		if (! action)
			break;

		if (action->type == ACTION_SYNC)
		{
			action_complete(action);
			continue;
		}

		// storage lock
		request_lock(req, LOCK_SHARED);

		auto db = catalog_find_db_by(catalog, &action->db, false);
		if (db && db->compute == &self->compute->task)
		{
			auto on_error = error_catch
			(
				if (action->type == ACTION_CHECKPOINT)
					apply_checkpoint(self, db, action);
				else
					apply_record(self, db, action);
			);
			if (on_error)
				service_error(service);
		}

		request_reset(req, true);
		action_complete(action);
	}
}

void
apply_init(Apply* self, Compute* compute)
{
	self->worker_id = -1;
	self->session   = NULL;
	self->compute   = compute;
	subscriber_init(&self->sub);
	request_init(&self->req);
	buf_init(&self->buf);
}

void
apply_free(Apply* self)
{
	if (self->session)
	{
		self->compute->iface->session_free(self->session);
		self->session = NULL;
	}
	request_free(&self->req);
	buf_free(&self->buf);
}

void
apply_start(Apply* self)
{
	service_attach(&share()->storage->service, &self->sub);

	self->worker_id = coroutine_create(apply_main, self);
}

void
apply_stop(Apply* self)
{
	service_detach(&share()->storage->service, &self->sub);

	coroutine_wait(self->worker_id);
	self->worker_id = -1;
}
