
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
primary_init(Primary*      self,
             Recover*      recover,
             Client*       client,
             PrimaryReplay replay,
             void*         replay_arg)
{
	self->client     = client;
	self->replay     = replay;
	self->replay_arg = replay_arg;
	self->recover    = recover;
}

static inline void
primary_write(Primary* self)
{
	auto client = self->client;
	auto reply  = &client->reply;
	auto id     = &config()->uuid.string;

	auto buf = http_begin_reply(reply, client->endpoint, "200 OK", 6, 0);
	buf_printf(buf, "Am-Id: %.*s\r\n", str_size(id), str_of(id));
	buf_printf(buf, "Am-Lsn: %" PRIu64 "\r\n", state_lsn());
	http_end(buf);
	tcp_write_buf(&client->tcp, buf);
}

static inline bool
primary_read(Primary* self)
{
	auto client  = self->client;
	auto request = &client->request;
	http_reset(request);
	auto eof = http_read(request, &client->readahead, false);
	if (eof)
		return true;
	http_read_content(request, &client->readahead, &request->content);
	return false;
}

void
primary_main(Primary* self)
{
	auto request = &self->client->request;
	info("primary connected.");

	// join
	self->replay(self, &request->content);

	// send current state
	primary_write(self);

	for (;;)
	{
		// read next write
		if (primary_read(self))
			break;

		// replay write
		cancel_pause();

		self->replay(self, &request->content);

		cancel_resume();

		// send current state
		primary_write(self);
	}
}

hot bool
primary_next(Primary* self)
{
	// this function must be called under session lock
	auto request = &self->client->request;

	// check replication state
	if (! opt_int_of(&state()->repl))
		error("replication is disabled");

	// validate primary id
	auto primary_id = &state()->repl_primary.string;
	if (str_empty(primary_id))
		error("server is not a replica");

	// Am-Id
	auto am_id = http_find(request, "Am-Id", 5);
	if (unlikely(! am_id))
		error("primary Am-Id field is missing");

	// validate primary id
	if (unlikely(! str_compare(&am_id->value, primary_id)))
		error("primary id mismatch");

	// first join request will have no data and no lsn field
	if (buf_empty(&request->content))
		return false;

	// Am-Lsn
	auto am_lsn = http_find(request, "Am-Lsn", 6);
	if (unlikely(! am_lsn))
		error("primary Am-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&am_lsn->value, &lsn) == -1))
		error("primary Am-Lsn is invalid");

	// lsn must much the current state
	if ((uint64_t)lsn != state_lsn())
		error("primary lsn does not match this server lsn");

	return true;
}
