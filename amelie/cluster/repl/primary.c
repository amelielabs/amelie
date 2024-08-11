
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
	auto id = &config()->uuid.string;
	http_write_reply(reply, 200, "OK");
	http_write(reply, "Amelie-Id", "%.*s", str_size(id), str_of(id));
	http_write(reply, "Amelie-Lsn", "%" PRIu64, config_lsn());
	http_write_end(reply);
	tcp_write_buf(&client->tcp, &reply->raw);
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
		self->replay(self, &request->content);

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
	if (! var_int_of(&config()->repl))
		error("replication is disabled");

	// validate primary id
	auto primary_id = &config()->repl_primary.string;
	if (str_empty(primary_id))
		error("server is not a replica");
	auto hdr_id = http_find(request, "Amelie-Id", 9);
	if (unlikely(! hdr_id))
		error("Amelie-Id field is missing");
	if (unlikely(! str_compare(&hdr_id->value, primary_id)))
		error("primary id mismatch");

	// first join request will have no data and no lsn field
	if (buf_empty(&request->content))
		return false;

	// validate lsn
	auto hdr_lsn = http_find(request, "Amelie-Lsn", 10);
	if (unlikely(! hdr_lsn))
		error("primary Amelie-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&hdr_lsn->value, &lsn) == -1))
		error("malformed replica Amelie-Lsn field");

	// lsn must much current state
	if ((uint64_t)lsn != config_lsn())
		error("primary lsn does not match this server lsn");

	return true;
}
