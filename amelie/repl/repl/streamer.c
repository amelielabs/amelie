
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
	auto client  = self->client;
	auto id      = &config()->uuid.string;

	// POST /
	//
	// application/octet-stream
	auto request = &client->request;
	auto buf = http_begin_request(request, client->endpoint, content? buf_size(content): 0);
	buf_printf(buf, "Am-Id: %.*s\r\n", str_size(id), str_of(id));
	if (content)
		buf_printf(buf, "Am-Lsn: %" PRIu64 "\r\n", self->wal_slot->lsn);
	http_end(buf);
	if (content)
		tcp_write_pair(&client->tcp, buf, content);
	else
		tcp_write_buf(&client->tcp, buf);
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

	// todo: validate endpoint /v1/repl

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
		auto reconnect_interval = (int)opt_int_of(&config()->repl_reconnect_ms);
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
		client_set_endpoint(self->client, self->endpoint);
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
		auto msg = task_recv();
		auto rpc = rpc_of(msg);
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
	self->endpoint      = NULL;
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
streamer_start(Streamer* self, Uuid* id, Endpoint* endpoint)
{
	uuid_get(id, self->replica_id, sizeof(self->replica_id));
	self->endpoint = endpoint;
	task_create(&self->task, "streamer", streamer_task_main, self);
}

void
streamer_stop(Streamer* self)
{
	rpc(&self->task, MSG_STOP, 0);
	task_wait(&self->task);
}
