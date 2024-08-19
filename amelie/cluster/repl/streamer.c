
//
// amelie.
//
// Real-Time SQL Database.
//

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_data.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_value.h>
#include <amelie_aggr.h>
#include <amelie_executor.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_backup.h>
#include <amelie_repl.h>

static inline bool
streamer_collect(Streamer* self)
{
	for (;;)
	{
		if (wal_cursor_collect(&self->wal_cursor, 256 * 1024, &self->lsn))
			break;

		// wait for new wal write, client disconnect or cancel
		Event on_event;
		event_init(&on_event);
		Event on_disconnect;
		event_init(&on_disconnect);
		event_set_parent(&on_disconnect, &on_event);
		event_set_parent(&self->wal_slot->on_write->event, &on_event);

		Exception e;
		if (enter(&e))
		{
			poll_read_start(&self->client->tcp.fd, &on_disconnect);
			event_wait(&on_event, -1);
		}

		event_set_parent(&self->wal_slot->on_write->event, NULL);
		poll_read_stop(&self->client->tcp.fd);

		if (leave(&e))
			rethrow();

		if (on_disconnect.signal)
			return false;
	}
	return true;
}

static inline void
streamer_write(Streamer* self, Buf* content)
{
	// push updates to the replica node
	auto client = self->client;
	auto request = &client->request;
	auto id = &config()->uuid.string;
	http_write_request(request, "POST /repl");
	http_write(request, "Content-Length", "%d", content ? buf_size(content) : 0);
	http_write(request, "Content-Type", "application/octet-stream");
	http_write(request, "Amelie-Id", "%.*s", str_size(id), str_of(id));
	if (content)
		http_write(request, "Amelie-Lsn", "%" PRIu64, self->wal_slot->lsn);
	http_write_end(request);

	if (content)
		tcp_write_pair(&client->tcp, &request->raw, content);
	else
		tcp_write_buf(&client->tcp, &request->raw);
}

static inline uint64_t
streamer_read(Streamer* self)
{
	auto client = self->client;

	// get replica state
	auto reply = &client->reply;
	http_reset(reply);
	auto eof = http_read(reply, &client->readahead, false);
	if (eof)
		error("unexpected eof");
	http_read_content(reply, &client->readahead, &reply->content);

	// validate replica id
	auto hdr_id = http_find(reply, "Amelie-Id", 9);
	if (unlikely(! hdr_id))
		error("replica Amelie-Id field is missing");
	if (unlikely(! str_compare_raw(&hdr_id->value, self->replica_id,
	             sizeof(self->replica_id) - 1)))
		error("replica Amelie-Id mismatch");

	// validate lsn
	auto hdr_lsn = http_find(reply, "Amelie-Lsn", 10);
	if (unlikely(! hdr_lsn))
		error("replica Amelie-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&hdr_lsn->value, &lsn) == -1))
		error("malformed replica Amelie-Lsn field");

	return lsn;
}

static void
streamer_connect(Streamer* self)
{
	client_connect(self->client);

	// POST / with empty content and current slot lsn
	streamer_write(self, NULL);

	// get replica state
	uint64_t lsn = streamer_read(self);

	// update wal slot according to the replica state

	// ensure replica is not outdated
	if (lsn > 0 && !wal_in_range(self->wal, lsn))
		error("replica is outdated");
	self->lsn = lsn;
	wal_slot_set(self->wal_slot, lsn);
	wal_attach(self->wal, self->wal_slot);

	// open cursor to the next record
	wal_cursor_open(&self->wal_cursor, self->wal, lsn + 1);

	// update streamer status
	atomic_u32_set(&self->connected, true);
}

static void
streamer_close(Streamer* self)
{
	atomic_u32_set(&self->connected, false);

	wal_detach(self->wal, self->wal_slot);
	wal_cursor_close(&self->wal_cursor);
	client_close(self->client);
}

static inline void
streamer_next(Streamer* self)
{
	// get replica state
	uint64_t lsn = streamer_read(self);

	// expect replica lsn to match last transmitted lsn record
	if (unlikely(lsn != self->lsn))
		error("replica lsn does not match last transmitted lsn");

	// update replication slot according to the replica state
	self->lsn = lsn;
	wal_slot_set(self->wal_slot, lsn);
}

static void
streamer_process(Streamer* self)
{
	for (;;)
	{
		Exception e;
		if (enter(&e))
		{
			streamer_connect(self);
			for (;;)
			{
				// collect and send wal records, read replica reply
				if (! streamer_collect(self))
					break;
				streamer_write(self, &self->wal_cursor.buf);
				streamer_next(self);
			}
		}

		if (leave(&e))
		{ }

		streamer_close(self);
		cancellation_point();

		// reconnect
		auto reconnect_interval = var_int_of(&config()->repl_reconnect_ms);
		info("reconnect in %d ms", reconnect_interval);
		coroutine_sleep(reconnect_interval);
	}
}

static void
streamer_main(void* arg)
{
	Streamer* self = arg;
	info("start");

	Exception e;
	if (enter(&e))
	{
		// create client, set node uri
		self->client = client_create();
		client_set_uri(self->client, self->replica_uri);
		streamer_process(self);
	}

	if (leave(&e))
	{ }

	if (self->client)
	{
		client_close(self->client);
		client_free(self->client);
		self->client = NULL;
	}

	info("stop");
}

static void
streamer_shutdown(Rpc* rpc, void* arg)
{
	unused(rpc);
	uint64_t* id = arg;
	coroutine_kill(*id);
}

static void
streamer_task_main(void* arg)
{
	Streamer* self = arg;
	uint64_t id = coroutine_create(streamer_main, self);
	for (;;)
	{
		auto buf = channel_read(&am_task->channel, -1);
		auto rpc = rpc_of(buf);
		guard(buf_free, buf);
		rpc_execute(rpc, streamer_shutdown, &id);
		break;
	}
}

void
streamer_init(Streamer* self, Wal* wal, WalSlot* wal_slot)
{
	self->client        = NULL;
	self->connected     = false;
	self->lsn           = 0;
	self->wal           = wal;
	self->wal_slot      = wal_slot;
	self->replica_id[0] = 0;
	self->replica_uri   = NULL;
	wal_cursor_init(&self->wal_cursor);
	task_init(&self->task);
	list_init(&self->link);
}

void
streamer_free(Streamer* self)
{
	task_free(&self->task);
}

void
streamer_start(Streamer* self, Uuid* id, Str* uri)
{
	uuid_to_string(id, self->replica_id, sizeof(self->replica_id));
	self->replica_uri = uri;
	task_create(&self->task, "streamer", streamer_task_main, self);
}

void
streamer_stop(Streamer* self)
{
	rpc(&self->task.channel, RPC_STOP, 0);
	task_wait(&self->task);
}
