
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
#include <amelie_sync.h>

void
node_init(Node* self, Db* db, Uuid* primary, Client* client)
{
	self->coroutine_id = am_self()->id;
	self->client       = client;
	self->primary      = primary;
	list_init(&self->link);
	recover_init(&self->recover, db, true);
}

void
node_free(Node* self)
{
	recover_free(&self->recover);
}

static void
node_replay(Node* self, Buf* batch)
{
	auto recover = &self->recover;

	// validate records and prepare for wal write
	auto lsn = state_lsn();
	auto pos = batch->start;
	auto end = batch->position;
	while (pos < end)
	{
		auto record = (Record*)pos;
		if (!record->lsn || record->size < sizeof(Record) ||
		    (uint8_t*)record + record->size > end)
				error("node: invalid record");

		if (opt_int_of(&config()->wal_crc))
			if (unlikely(! record_validate(record)))
				error("node: record crc mismatch");

		if (record->lsn != (lsn + 1))
			error("node: next lsn mismatch");

		lsn++;
		pos += record->size;
	}

	// wal write (update global lsn)
	db_write_recover(self->recover.db, batch, lsn);

	// execute
	pos = batch->start;
	while (pos < end)
	{
		auto record = (Record*)pos;

		auto buf = buf_create();
		buf_emplace(buf, sizeof(RecordMsg) + record->size);

		auto msg = (RecordMsg*)buf->start;
		msg_init(&msg->msg, MSG_RECORD);
		msg->msg_buf = buf;
		msg->arg     = NULL;
		memcpy(msg->record, record, record->size);

		// execute
		recover_next(recover, msg);

		pos += record->size;
	}

	// wait for commit
	recover_wait(recover);
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

		if (! uuid_is(&msg.id, self->primary))
			error("node: primary id mismatch");

		// replay batch write
		if (! buf_empty(buf))
		{
			cancel_pause();
			node_replay(self, buf);
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

	error_catch
	(
		node_process(self)
	);

	recover_wait(&self->recover);
}
