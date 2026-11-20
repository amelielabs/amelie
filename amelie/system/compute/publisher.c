
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
publisher_read(Publisher* self, Publish* msg)
{
	// set replay
	auto req = &self->req;

	// set relation name
	auto pos = msg->msg_buf->start;
	unpack_str(&pos, &req->rel);

	// content_type left empty for json

	// set content
	auto start = pos;
	data_skip(&pos);
	str_set_u8(&req->content, start, pos - start);
}

static void
publisher_execute(Publisher* self, Publish* msg)
{
	defer_buf(msg->msg_buf);

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

	// process request
	error_catch
	(	
		// set request
		portal_prepare(portal);
		publisher_read(self, msg);

		// set db (takes shared storage lock)
		portal_bind(portal, &msg->db);

		// todo:
			// db.ack = msg.lsn

		// execute
		compute->iface->session_execute(self->session, portal, &self->req);
	);

	portal_reset(portal, true);
}

static void
publisher_main(void* arg)
{
	Publisher* self = arg;
	for (;;)
	{
		auto msg = mailbox_pop(&self->queue, am_self());
		if (msg->id == MSG_STOP)
			break;

		// execute
		publisher_execute(self, publish_of(msg));
	}
}

void
publisher_init(Publisher* self, Compute* compute)
{
	self->session   = NULL;
	self->worker_id = -1;
	self->compute   = compute;
	mailbox_init(&self->queue);
	portal_init(&self->portal);
	request_init(&self->req);
	buf_init(&self->buf);
}

void
publisher_free(Publisher* self)
{
	// todo: free mailbox

	if (self->session)
	{
		self->compute->iface->session_free(self->session);
		self->session = NULL;
	}
	portal_free(&self->portal);
	buf_free(&self->buf);
}

void
publisher_start(Publisher* self)
{
	if (self->worker_id != -1)
		return;
	self->worker_id = coroutine_create(publisher_main, self);
}

void
publisher_stop(Publisher* self)
{
	if (self->worker_id == -1)
		return;
	auto worker = coroutines_find(&am_task->coroutines, self->worker_id);
	assert(worker);
	Msg stop;
	msg_init(&stop, MSG_STOP);
	publisher_add(self, &stop);

	wait_event(&worker->on_exit, am_self());
	self->worker_id = -1;
}

void
publisher_add(Publisher* self, Msg* msg)
{
	mailbox_append(&self->queue, msg);
	event_signal(&self->queue.event);
}
