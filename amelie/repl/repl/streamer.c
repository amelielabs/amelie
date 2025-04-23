
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

#include <amelie_runtime.h>
#include <amelie_io.h>
#include <amelie_lib.h>
#include <amelie_json.h>
#include <amelie_config.h>
#include <amelie_user.h>
#include <amelie_auth.h>
#include <amelie_http.h>
#include <amelie_client.h>
#include <amelie_server.h>
#include <amelie_row.h>
#include <amelie_heap.h>
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>

static inline bool
streamer_collect(Streamer* self)
{
	auto size = opt_int_of(&config()->repl_readahead);
	for (;;)
	{
		auto lsn_max = state_lsn();
		if (wal_cursor_readahead(&self->wal_cursor, size, &self->lsn, lsn_max) > 0)
			break;

		// wait for new wal write, client disconnect or cancel
		Event on_event;
		event_init(&on_event);
		Event on_disconnect;
		event_init(&on_disconnect);
		event_set_parent(&on_disconnect, &on_event);
		event_set_parent(&self->wal_slot->on_write, &on_event);

		auto on_error = error_catch
		(
			poll_read_start(&self->client->tcp.fd, &on_disconnect);
			event_wait(&on_event, -1);
		);

		event_set_parent(&self->wal_slot->on_write, NULL);
		poll_read_stop(&self->client->tcp.fd);

		if (on_error)
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
	auto token = remote_get(self->remote, REMOTE_TOKEN);
	http_write_request(request, "POST /");
	if (! str_empty(token))
		http_write(request, "Authorization", "Bearer %.*s", str_size(token), str_of(token));
	http_write(request, "Content-Length", "%d", content ? buf_size(content) : 0);
	http_write(request, "Content-Type", "application/octet-stream");
	http_write(request, "Am-Service", "repl");
	http_write(request, "Am-Version", "1");
	http_write(request, "Am-Id", "%.*s", str_size(id), str_of(id));
	if (content)
		http_write(request, "Am-Lsn", "%" PRIu64, self->wal_slot->lsn);
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
	if (! str_is(&reply->options[HTTP_CODE], "200", 3))
		error("unexpected reply code");
	http_read_content(reply, &client->readahead, &reply->content);

	// Am-Service
	auto am_service = http_find(reply, "Am-Service", 10);
	if (unlikely(! am_service))
		error("replica Am-Service field is missing");
	if (unlikely(! str_is(&am_service->value, "repl", 4)))
		error("replica Am-Service is invalid");

	// Am-Version
	auto am_version = http_find(reply, "Am-Version", 10);
	if (unlikely(! am_version))
		error("replica Am-Version field is missing");
	if (unlikely(! str_is(&am_version->value, "1", 1)))
		error("replica Am-Version is invalid");

	// Am-Id
	auto am_id = http_find(reply, "Am-Id", 5);
	if (unlikely(! am_id))
		error("replica Am-Id field is missing");
	if (unlikely(! str_is(&am_id->value, self->replica_id,
	             sizeof(self->replica_id) - 1)))
		error("replica Am-Id mismatch");

	// Am-Lsn
	auto am_lsn = http_find(reply, "Am-Lsn", 6);
	if (unlikely(! am_lsn))
		error("replica Am-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&am_lsn->value, &lsn) == -1))
		error("replica Am-Lsn is invalid");

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
	wal_cursor_open(&self->wal_cursor, self->wal, lsn + 1, true, false);

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
streamer_client(Streamer* self)
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

static void
streamer_process(Streamer* self)
{
	for (;;)
	{
		// connect
		error_catch( streamer_client(self) );
		streamer_close(self);

		cancellation_point();

		// reconnect
		auto reconnect_interval = opt_int_of(&config()->repl_reconnect_ms);
		info("reconnect in %d ms", reconnect_interval);
		coroutine_sleep(reconnect_interval);
	}
}

static void
streamer_main(void* arg)
{
	Streamer* self = arg;
	info("start");

	// create client, set node uri
	error_catch
	(
		self->client = client_create();
		client_set_remote(self->client, self->remote);
		streamer_process(self);
	);

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
		defer(buf_free, buf);
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
	self->remote        = NULL;
	self->replica_id[0] = 0;
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
streamer_start(Streamer* self, Uuid* id, Remote* remote)
{
	uuid_get(id, self->replica_id, sizeof(self->replica_id));
	self->remote = remote;
	task_create(&self->task, "streamer", streamer_task_main, self);
}

void
streamer_stop(Streamer* self)
{
	rpc(&self->task.channel, RPC_STOP, 0);
	task_wait(&self->task);
}
