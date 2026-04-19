
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

static inline bool
streamer_collect(Streamer* self)
{
	auto size = opt_int_of(&config()->repl_readahead);
	for (;;)
	{
		auto lsn_max = state_lsn();
		if (wal_cursor_readahead(&self->wal_cursor, size, &self->lsn, lsn_max) > 0)
			break;

		Event event;
		event_init(&event);
		event_attach(&event);

		// subscribe for wal writes
		WalSub sub;
		wal_sub_init(&sub, &event, self->lsn);
		wal_subscribe(self->wal, &sub);

		// wait for wal write, client disconnect or cancel
		Event on_disconnect;
		event_init(&on_disconnect);
		event_set_parent(&on_disconnect, &event);
		auto on_error = error_catch
		(
			poll_read_start(&self->client->tcp.fd, &on_disconnect);
			event_wait(&event, -1);
		);
		poll_read_stop(&self->client->tcp.fd);
		wal_unsubscribe(self->wal, &sub);

		if (on_error)
			rethrow();

		if (on_disconnect.signal)
			return false;
	}
	return true;
}

static inline void
streamer_write(Streamer* self, uint64_t lsn, Buf* content)
{
	// NODE_WRITE
	node_send(&self->websocket, NODE_WRITE, &self->id_primary, lsn, content);
}

static inline uint64_t
streamer_read(Streamer* self)
{
	// NODE_ACK
	NodeMsg msg;
	if (! node_recv(&self->websocket, &msg, NULL))
		error("streamer: unexpected eof");
	if (msg.op != NODE_ACK)
		error("streamer: unexpected replica response");
	if (uuid_compare(&msg.id, &self->id_replica))
		error("streamer: replica id mismatch");
	return msg.lsn;
}

static void
streamer_connect(Streamer* self)
{
	// POST /v1/repl
	auto websocket = &self->websocket;
	opt_int_set(&websocket->client->endpoint->endpoint, ENDPOINT_REPL);
	client_connect(self->client);

	// do websocket handshake
	websocket_connect(websocket);

	// NODE_WRITE (empty write request)
	streamer_write(self, 0, NULL);

	// get replica state
	uint64_t lsn = streamer_read(self);

	// update wal slot according to the replica state

	// ensure replica is not outdated
	if (lsn > 0)
	{
		auto min = wal_find(self->wal, 0, false);
		assert(min);
		defer(wal_file_unpin_defer, min);
		if (min->id > lsn)
			error("replica is outdated");
	}

	self->lsn = lsn;
	wal_slot_set(self->wal_slot, lsn);

	// open cursor to the next record
	wal_cursor_open(&self->wal_cursor, self->wal, lsn + 1, true, false);

	// update streamer status
	atomic_u32_set(&self->connected, true);
}

static void
streamer_close(Streamer* self)
{
	atomic_u32_set(&self->connected, false);

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
		streamer_write(self, self->wal_slot->lsn, &self->wal_cursor.buf);
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

		// set websocket
		Str protocol;
		str_set(&protocol, "amelie-v1-repl", 14);
		websocket_set(&self->websocket, &protocol, self->client, true);

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
	self->client    = NULL;
	self->connected = false;
	self->lsn       = 0;
	self->wal       = wal;
	self->wal_slot  = wal_slot;
	self->endpoint  = NULL;
	websocket_init(&self->websocket);
	uuid_init(&self->id_primary);
	uuid_init(&self->id_replica);
	wal_cursor_init(&self->wal_cursor);
	task_init(&self->task);
	list_init(&self->link);
}

void
streamer_free(Streamer* self)
{
	websocket_free(&self->websocket);
	task_free(&self->task);
}

void
streamer_start(Streamer* self, Uuid* id_replica, Endpoint* endpoint)
{
	uuid_set(&self->id_primary, &config()->uuid.string);
	self->id_replica = *id_replica;
	self->endpoint   = endpoint;
	task_create(&self->task, "streamer", streamer_task_main, self);
}

void
streamer_stop(Streamer* self)
{
	rpc(&self->task, MSG_STOP, 0);
	task_wait(&self->task);
}
