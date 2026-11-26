
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

static inline void
service_process(Service* self, Write* record)
{
	auto req      = &self->req;
	auto endpoint = &req->endpoint;

	// restore request
	write_read(record_data(record->record),
	           &endpoint->db_id,
	           &endpoint->timezone,
	           &endpoint->time,
	           &endpoint->seed,
	           &endpoint->rel,
	           &endpoint->content_type,
	           &endpoint->content);

	// recover
	if (record->msg.id == MSG_RECORD)
		req->replay = true;

	// set db (takes shared storage lock)
	request_bind(req, &endpoint->db_id);

	// execute
	auto compute = self->compute;
	compute->iface->session_execute(self->session, req);
}

static void
service_execute(Service* self, Write* record)
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

	// process request
	auto on_error = error_catch (
		service_process(self, record)
	);

	request_reset(req, true);

	buf_free(record->msg_buf);

	// sync
	if (record->msg.id == MSG_RECORD)
	{
		RecoverSync* sync = record->arg;
		if (on_error)
			recover_sync_add_error(sync);
		recover_sync_add_complete(sync);
	}
}

static void
service_checkpoint(Rpc* rpc, void* arg)
{
	// create db checkpoint
	unused(arg);
	void**    argv = rpc->arg;
	Db*       db = argv[0];
	uint64_t* checkpoint = argv[1];
	storage_checkpoint_db(share()->storage, db);
	*checkpoint = db->checkpoints.current->id;
}

static void
service_main(void* arg)
{
	Service* self = arg;

	// main
	for (;;)
	{
		auto msg = mailbox_pop(&self->mailbox, am_self());

		if (msg->id == MSG_STOP)
			break;

		// checkpoint
		if (msg->id == MSG_CHECKPOINT)
		{
			auto rpc = rpc_of(msg);
			rpc_execute(rpc, service_checkpoint, self);
			continue;
		}

		// replay
		service_execute(self, (Write*)msg);
	}
}

void
service_init(Service* self, Compute* compute)
{
	self->worker_id = -1;
	self->session   = NULL;
	self->compute   = compute;
	mailbox_init(&self->mailbox);
	request_init(&self->req);
	buf_init(&self->buf);
}

void
service_free(Service* self)
{
	// todo: free mailbox?
	if (self->session)
	{
		self->compute->iface->session_free(self->session);
		self->session = NULL;
	}
	request_free(&self->req);
	buf_free(&self->buf);
}

void
service_start(Service* self)
{
	self->worker_id = coroutine_create(service_main, self);
}

void
service_stop(Service* self)
{
	Msg msg;
	msg_init(&msg, MSG_STOP);
	service_add(self, &msg);

	coroutine_wait(self->worker_id);
	self->worker_id = -1;
}
