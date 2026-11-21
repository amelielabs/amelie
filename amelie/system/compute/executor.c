
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
executor_read(Executor* self, Action* action)
{
	// set replay
	auto req = &self->req;

	// set relation name
	auto pos = action->data;
	unpack_str(&pos, &req->rel);

	// content_type left empty for json

	// set content
	auto start = pos;
	data_skip(&pos);
	str_set_u8(&req->content, start, pos - start);
}

static void
executor_run(Executor* self, Action* action)
{
	auto compute = self->compute;
	auto iface   = self->compute->iface;
	if (! self->session)
		self->session = iface->session_create(compute, compute->iface_arg);

	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;
	request_reset(&self->req);

	// todo: output to none
	buf_reset(&self->buf);
	output_set_buf(&portal->output, &self->buf);
	output_set(&portal->output, endpoint, &output_json, NULL);

	// set request
	portal_prepare(portal);
	executor_read(self, action);

	// take storage lock
	portal_lock(portal, LOCK_SHARED);

	info(" -- action");

	auto dbs = &share()->storage->catalog.rels.list;
	list_foreach(dbs)
	{
		auto db = db_of(list_at(Rel, link));
		if (db->compute != am_task)
			continue;

		// todo: db.ack = msg.lsn
		if (! uuid_is(&action->filter, db->rel.id))
			continue;

		// execute
		error_catch
		(
			portal_set(portal, db, true);
			compute->iface->session_execute(self->session, portal, &self->req);
		);
	}

	portal_reset(portal, true);
}

static void
executor_main(void* arg)
{
	Executor* self = arg;

	// register subscriber
	auto service = &share()->storage->service;
	auto sub = &self->sub;
	service_attach(service, sub);

	// main
	for (;;)
	{
		auto action = service_next(service, sub);
		if (! action)
			break;
		executor_run(self, action);
	}
}

void
executor_init(Executor* self, Compute* compute)
{
	self->session = NULL;
	self->compute = compute;
	subscriber_init(&self->sub);
	portal_init(&self->portal);
	request_init(&self->req);
	buf_init(&self->buf);
}

void
executor_free(Executor* self)
{
	if (self->session)
	{
		self->compute->iface->session_free(self->session);
		self->session = NULL;
	}
	portal_free(&self->portal);
	buf_free(&self->buf);
}

void
executor_start(Executor* self)
{
	coroutine_create(executor_main, self);
}
