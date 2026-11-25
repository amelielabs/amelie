
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
service_ack(Service* self, uint64_t ack)
{
	auto db = self->portal.db;
	db_ack(db, ack);

	if (! db->publish_count)
		return;

	list_foreach_safe(&db->publish)
	{
		auto write = list_at(Write, msg.link);
		if (write->record->lsn <= ack)
		{
			list_unlink(&write->msg.link);
			db->publish_count--;
			buf_free(write->msg_buf);
		}
	}
}

static inline bool
service_process(Service* self, Write* record)
{
	auto portal = &self->portal;
	auto endpoint = &portal->endpoint;

	// restore request
	request_read(&portal->req, endpoint, record);

	// set db (takes shared storage lock)
	portal_bind(portal, &endpoint->db_id);
	auto db = portal->db;

	// process
	bool free    = true;
	bool execute = true;
	switch (record->msg.id) {
	case MSG_PUBLISH:
	{
		// publish request (runtime)

		// set db ack (will be part of the upcoming wal write)
		db_ack(db, record->record->lsn);
		break;
	}
	case MSG_RECORD:
	{
		// RECOVERY (WAL or REPL)
		if (record->record->flags & RECORD_PUBLISH)
		{
			// if publish is not already processed then
			// add to the queue
			if (db_ack(db, record->record->lsn))
			{
				list_append(&db->publish, &record->msg.link);
				db->publish_count++;

				free = false;
			}
			execute = false;
		}

		// ACK (gc queue <= ack)
		service_ack(self, record->record->ack);
		break;
	}
	default:
		abort();
	}

	// execute
	if (execute)
	{
		auto compute = self->compute;
		compute->iface->session_execute(self->session, portal);
	}

	return free;
}

static void
service_execute(Service* self, Write* record)
{
	auto compute = self->compute;
	auto iface   = self->compute->iface;
	if (! self->session)
		self->session = iface->session_create(compute, compute->iface_arg);

	auto portal   = &self->portal;
	auto endpoint = &portal->endpoint;

	// todo: output to none
	buf_reset(&self->buf);
	output_set_buf(&portal->output, &self->buf);
	output_set(&portal->output, endpoint, &output_json, NULL);

	// process request
	bool free;
	auto on_error = error_catch
	(
		free = service_process(self, record)
	);

	portal_reset(portal, true);

	if (free)
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
	portal_init(&self->portal);
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
	portal_free(&self->portal);
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
