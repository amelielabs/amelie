
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
#include <amelie_repl.h>

void
node_init(Node* self, Db* db, RecoverIf* iface, void* iface_arg, Client* client)
{
	self->coroutine_id = am_self()->id;
	self->client = client;
	list_init(&self->link);
	recover_init(&self->recover, db, iface, iface_arg);
}

void
node_free(Node* self)
{
	recover_free(&self->recover);
}

static void
node_replay(Node* self, NodeMsg* node_msg, Buf* data)
{
	auto recover = &self->recover;

	// replay writes
	auto pos = data->start;
	auto end = data->position;
	while (pos < end)
	{
		auto record = (Record*)pos;
		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate(record)))
				error("node: record crc mismatch");

		auto buf = buf_create();
		buf_emplace(buf, sizeof(RecordMsg) + record->size);

		auto msg = (RecordMsg*)buf->start;
		msg_init(&msg->msg, MSG_RECORD);
		msg->msg_buf     = buf;
		msg->arg         = NULL;
		msg->instance_id = node_msg->id;
		memcpy(msg->record, record, record->size);

		// execute
		recover->iface->execute(recover, msg);

		pos += record->size;
	}

	// wait for commit
	recover->iface->sync(recover);
}

static void
node_process(Node* self)
{
	auto client = self->client;
	auto buf = &client->request.content;
	for (;;)
	{
		// NODE_WRITE
		buf_reset(buf);

		NodeMsg msg;
		if (! node_recv(client, &msg, buf))
			break;
		if (msg.op != NODE_WRITE)
			error("node: unexpected message");

		// replay batch write
		if (! buf_empty(buf))
		{
			cancel_pause();
			node_replay(self, &msg, buf);
			cancel_resume();
		}

		// NODE_ACK
		node_send(client, NODE_ACK, opt_uuid_of(&config()->uuid),
		          state_lsn(), NULL);
	}
}

void
node_main(Node* self)
{
	info("node connected.");

	// prepare recovery state
	self->recover.iface->create(&self->recover);

	error_catch (
		node_process(self);
	);

	self->recover.iface->sync(&self->recover);
}
