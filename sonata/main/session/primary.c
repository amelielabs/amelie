
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
#include <sonata_semantic.h>
#include <sonata_compiler.h>
#include <sonata_backup.h>
#include <sonata_repl.h>
#include <sonata_cluster.h>
#include <sonata_frontend.h>
#include <sonata_session.h>

static void
replay_read(Session* self, WalWrite* write, ReqList* req_list)
{
	auto router = &self->share->cluster->router;

	// redistribute DML operations/rows between nodes
	Req* map[router->list_count];
	memset(map, 0, sizeof(map));

	// [header][meta][data]
	auto start = (uint8_t*)write;
	auto meta  = start + sizeof(*write);
	auto data  = meta + write->offset;
	for (auto i = write->count; i > 0; i--)
	{
		auto meta_start = meta;

		// type
		data_skip(&meta);

		// partition id
		int64_t partition_id;
		data_read_integer(&meta, &partition_id);

		// map each write to route
		auto table_mgr = &self->share->db->table_mgr;
		auto part = table_mgr_find_partition(table_mgr, partition_id);
		if (! part)
			error("failed to find partition %" PRIu64, partition_id);
		auto route = part->route;
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(self->plan.req_cache);
			req->arg_start = start;
			req->route     = route;
			req_list_add(req_list, req);
			map[route->order] = req;
		}

		// todo: serial recover?

		// [meta offset, data offset]
		encode_integer(&req->arg, (intptr_t)(meta_start - start));
		encode_integer(&req->arg, (intptr_t)(data - start));
		data_skip(&data);
	}
}

static void
replay(Session* self, WalWrite* write)
{
	auto executor = self->share->executor;
	auto plan = &self->plan;
	plan_reset(plan);
	plan_create(plan, NULL, NULL, 1, 0);

	ReqList req_list;
	req_list_init(&req_list);
	guard(req_list_free, &req_list);

	Exception e;
	if (enter(&e))
	{
		replay_read(self, write, &req_list);

		executor_send(executor, plan, 0, &req_list);
		executor_recv(executor, plan, 0);
	}
	if (leave(&e))
	{
		plan_shutdown(plan);
		auto buf = msg_error(&so_self()->error);
		plan_set_error(plan, buf);
	}

	executor_complete(executor, plan);
	executor_commit(executor, plan);
}

hot static void
on_write(Primary* self, Buf* data)
{
	Session* session = self->replay_arg;

	// take shared lock
	session_lock(session, SESSION_LOCK);
	guard(session_unlock, session);

	// validate request fields and check current replication state

	// first join request has no data
	if (! primary_next(self))
		return;

	// replay writes
	auto pos = data->start;
	auto end = data->position;
	while (pos < end)
	{
		auto write = (WalWrite*)pos;
		if ((write->flags & WAL_UTILITY) > 0)
		{
			// upgrade to exclusive lock
			session_lock(session, SESSION_LOCK_EXCLUSIVE);

			// read and execute ddl command
			recover_next_write(self->recover, write, true, WAL_UTILITY);	
		} else
		{
			// execute dml commands
			replay(session, write);
		}
		pos += write->size;
	}
}

hot void
session_primary(Session* self)
{
	auto share = self->share;

	// switch plan to replication state to write wal
	// while in read-only mode
	plan_set_repl(&self->plan);

	Recover recover;
	recover_init(&recover, share->db, indexate_runner, share->cluster);
	guard(recover_free, &recover);

	Primary primary;
	primary_init(&primary, &recover, self->client, on_write, self);
	primary_main(&primary);
}
