
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
#include <sonata_user.h>
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
#include <sonata_repl.h>

static inline void
pusher_collect(Pusher* self)
{
	for (;;)
	{
		if (wal_cursor_collect(&self->wal_cursor, 256 * 1024, &self->lsn))
			break;
		// todo: disconnect (timeout to ping or check error?)
		wal_slot_wait(self->wal_slot);
	}
}

static inline void
pusher_write(Pusher* self)
{
	// push updates to the replica node
	auto client = self->client;
	auto request = &client->request;
	auto content = &self->wal_cursor.buf;
	http_write_request(request, "POST /");
	http_write(request, "Sonata-Id", "%s", self->id);
	http_write(request, "Sonata-Lsn", "%" PRIu64, self->wal_slot->lsn);
	http_write(request, "Content-Length", "%d", content ? buf_size(content) : 0);
	http_write(request, "Content-Type", "application/octet-stream");
	http_write_end(request);
	if (content)
		tcp_write_pair(&client->tcp, &request->raw, content);
	else
		tcp_write_buf(&client->tcp, &request->raw);
}

static inline bool
pusher_read(Pusher* self)
{
	auto client = self->client;

	// get replica state
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		return true;
	http_read_content(reply, &client->readahead, &reply->content);

	// validate id
	auto hdr_id = http_find(reply, "Sonata-Id", 9);
	if (unlikely(! hdr_id))
		error("replica Sonata-Id field is missing");
	if (! str_compare_raw(&hdr_id->value, self->id, sizeof(self->id) - 1))
		error("replica Sonata-Id mismatch");

	// validate lsn
	auto hdr_lsn = http_find(reply, "Sonata-Lsn", 10);
	if (unlikely(! hdr_lsn))
		error("replica Sonata-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&hdr_lsn->value, &lsn) == -1))
		error("malformed replica Sonata-Lsn field");

	// expect replica lsn to match last transmitted lsn record
	if (unlikely((uint64_t)lsn != self->lsn))
		error("replica lsn does not match last transmitted lsn");

	// update replication slot according to the replica state
	self->lsn = lsn;
	wal_slot_set(self->wal_slot, lsn);
	return false;
}

static void
pusher_main(void* arg)
{
	Pusher* self = arg;
	auto tcp = &self->client->tcp;

	Exception e;
	if (enter(&e))
	{
		tcp_attach(tcp);

		wal_attach(self->wal, self->wal_slot);
		wal_cursor_open(&self->wal_cursor, self->wal, self->wal_slot->lsn + 1);
		for (;;)
		{
			// collect and send wal records, read next request
			pusher_collect(self);
			pusher_write(self);
			if (pusher_read(self))
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
pusher_task_main(void* arg)
{
	Pusher* self = arg;
	uint64_t id = coroutine_create(pusher_main, self);
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
pusher_init(Pusher* self, Wal* wal, WalSlot* wal_slot, Uuid* id)
{
	self->client      = NULL;
	self->lsn         = 0;
	self->wal         = wal;
	self->wal_slot    = wal_slot;
	self->on_complete = NULL;
	uuid_to_string(id, self->id, sizeof(self->id));
	wal_cursor_init(&self->wal_cursor);
	task_init(&self->task);
}

void
pusher_free(Pusher* self)
{
	if (self->on_complete)
		condition_free(self->on_complete);
	task_free(&self->task);
}

void
pusher_start(Pusher* self, Client* client)
{
	tcp_detach(&client->tcp);
	self->client = client;

	self->on_complete = condition_create();
	task_create(&self->task, "pusher", pusher_task_main, self);
}

void
pusher_stop(Pusher* self)
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
