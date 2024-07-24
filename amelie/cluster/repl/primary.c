
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
#include <sonata_row.h>
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
#include <sonata_planner.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>

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
	http_write(reply, "Sonata-Id", "%.*s", str_size(id), str_of(id));
	http_write(reply, "Sonata-Lsn", "%" PRIu64, config_lsn());
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
	log("primary connected.");

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
	auto hdr_id = http_find(request, "Sonata-Id", 9);
	if (unlikely(! hdr_id))
		error("Sonata-Id field is missing");
	if (unlikely(! str_compare(&hdr_id->value, primary_id)))
		error("primary id mismatch");

	// first join request will have no data and no lsn field
	if (buf_empty(&request->content))
		return false;

	// validate lsn
	auto hdr_lsn = http_find(request, "Sonata-Lsn", 10);
	if (unlikely(! hdr_lsn))
		error("primary Sonata-Lsn field is missing");
	int64_t lsn;
	if (unlikely(str_toint(&hdr_lsn->value, &lsn) == -1))
		error("malformed replica Sonata-Lsn field");

	// lsn must much current state
	if ((uint64_t)lsn != config_lsn())
		error("primary lsn does not match this server lsn");

	return true;
}
