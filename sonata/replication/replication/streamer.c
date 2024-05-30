
//
// sonata.
//
// Real-Time SQL Database.
//

#include <sonata_runtime.h>
#include <sonata_io.h>
#include <sonata_lib.h>
#include <sonata_data.h>
#include <sonata_config.h>
#include <sonata_auth.h>
#include <sonata_http.h>
#include <sonata_client.h>
#include <sonata_server.h>
#include <sonata_def.h>
#include <sonata_transaction.h>
#include <sonata_index.h>
#include <sonata_partition.h>
#include <sonata_wal.h>
#include <sonata_db.h>
#include <sonata_value.h>
#include <sonata_aggr.h>
#include <sonata_executor.h>
#include <sonata_vm.h>
#include <sonata_parser.h>
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_replication.h>

void
streamer_init(Streamer* self, Wal* wal)
{
	self->client      = NULL;
	self->lsn         = 0;
	self->lsn_last    = 0;
	self->id          = NULL;
	self->wal_slot    = NULL;
	self->wal         = wal;
	self->on_complete = NULL;
	wal_cursor_init(&self->wal_cursor);
	task_init(&self->task);
}

void
streamer_free(Streamer* self)
{
	if (self->on_complete)
		condition_free(self->on_complete);
	task_free(&self->task);
}

static inline void
streamer_collect(Streamer* self)
{
	for (;;)
	{
		if (wal_cursor_collect(&self->wal_cursor, 256 * 1024, &self->lsn_last))
			break;
		// todo: tcp disconnect	
		wal_slot_wait(self->wal_slot);
	}
}

static inline void
streamer_write(Streamer* self)
{
	auto client = self->client;
	auto content = &self->wal_cursor.buf;
	auto reply = &client->reply;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Content-Length", "%d", buf_size(content));
	http_write(reply, "Content-Type", "application/octet-stream");
	http_write(reply, "Sonata-Repl", "%s", self->id_sz);
	http_write(reply, "Sonata-Repl-Lsn", "%" PRIu64, self->lsn);
	http_write_end(reply);
	tcp_write_pair(&client->tcp, &reply->raw, content);
}

static inline bool
streamer_read(Streamer* self)
{
	auto client    = self->client;
	auto readahead = &client->readahead;

	// process next request
	auto request = &client->request;
	auto eof = http_read(request, readahead, true);
	if (eof)
		return true;
	http_read_content(request, readahead, &request->content);

	// validate headers
	auto repl_id  = http_find(request, "Sonata-Repl", 11);
	auto repl_lsn = http_find(request, "Sonata-Repl-Lsn", 15);
	if (unlikely(! (repl_id || repl_lsn)))
		error("replica header fields are missing");

	// compare replica id
	Uuid id;
	uuid_from_string(&id, &repl_id->value);
	if (unlikely(! uuid_compare(&id, self->id)))
		error("replica id mismatch");

	// expect replica lsn to match last transmitted lsn record
	int64_t lsn;
	if (unlikely(str_toint(&repl_lsn->value, &lsn) == -1))
		error("malformed lsn value");
	if (unlikely(lsn != (int64_t)self->lsn_last))
		error("replica lsn does not match last transmitted lsn");

	// update slot
	self->lsn = self->lsn_last;
	wal_slot_set(self->wal_slot, self->lsn);
	return false;
}

static void
streamer_main(void* arg)
{
	Streamer* self = arg;
	auto tcp = &self->client->tcp;

	Exception e;
	if (enter(&e))
	{
		tcp_attach(tcp);
		wal_attach(self->wal, self->wal_slot);
		wal_cursor_open(&self->wal_cursor, self->wal, self->lsn);
		for (;;)
		{
			// collect and send wal records, read next request
			streamer_collect(self);
			streamer_write(self);
			if (streamer_read(self))
				break;
		}
	}

	if (leave(&e))
	{ }

	wal_detach(self->wal, self->wal_slot);
	wal_cursor_close(&self->wal_cursor);

	tcp_close(tcp);

	condition_signal(self->on_complete);
	log("complete");
}

static void
streamer_task_main(void* arg)
{
	Streamer* self = arg;
	uint64_t id = coroutine_create(streamer_main, self);
	for (;;)
	{
		auto buf = channel_read(&so_task->channel, -1);
		auto msg = msg_of(buf);
		guard(buf_free, buf);
		if (msg->id == RPC_STOP)
		{
			coroutine_kill(id);
			break;
		}
	}
}

void
streamer_start(Streamer* self, Client* client, WalSlot* slot, Uuid* id)
{
	self->client      = client;
	self->lsn         = slot->lsn;
	self->lsn_last    = slot->lsn;
	self->id          = id;
	self->wal_slot    = slot;
	self->on_complete = condition_create();

	uuid_to_string(id, self->id_sz, sizeof(self->id_sz));
	task_create(&self->task, "streamer", streamer_task_main, self);
}

void
streamer_stop(Streamer* self)
{
	// sent stop
	auto buf = msg_begin(RPC_STOP);
	msg_end(buf);
	channel_write(&self->task.channel, buf);

	// wait for completion
	coroutine_cancel_pause(so_self());
	condition_wait(self->on_complete, -1);
	coroutine_cancel_resume(so_self());

	task_wait(&self->task);
}
