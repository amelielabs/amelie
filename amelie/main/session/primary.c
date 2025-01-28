
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
#include <amelie_transaction.h>
#include <amelie_index.h>
#include <amelie_partition.h>
#include <amelie_checkpoint.h>
#include <amelie_wal.h>
#include <amelie_db.h>
#include <amelie_backup.h>
#include <amelie_repl.h>
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_planner.h>
#include <amelie_compiler.h>
#include <amelie_cluster.h>
#include <amelie_frontend.h>
#include <amelie_session.h>

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
		json_skip(&meta);

		// partition id
		int64_t partition_id;
		json_read_integer(&meta, &partition_id);

		// map each write to route
		auto table_mgr = &self->share->db->table_mgr;
		auto part = table_mgr_find_partition(table_mgr, partition_id);
		if (! part)
			error("failed to find partition %" PRIu64, partition_id);
		auto route = part->route;
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&self->dtr.req_cache, REQ_REPLAY);
			req->arg_start = start;
			req->route     = route;
			req_list_add(req_list, req);
			map[route->order] = req;
		}

		// todo: serial recover?

		// [meta offset, data offset]
		encode_integer(&req->arg, (intptr_t)(meta_start - start));
		encode_integer(&req->arg, (intptr_t)(data - start));
		data += row_size((Row*)data);
	}
}

static void
replay(Session* self, WalWrite* write)
{
	auto executor = self->share->executor;
	auto dtr = &self->dtr;

	// switch distributed transaction to replication state to write wal
	// while in read-only mode
	Program program;
	program_init(&program);
	program.stmts      = 1;
	program.stmts_last = 0;
	program.repl       = true;

	dtr_reset(dtr);
	dtr_create(dtr, &program, NULL);

	ReqList req_list;
	req_list_init(&req_list);
	defer(req_list_free, &req_list);

	auto on_error = error_catch
	(
		replay_read(self, write, &req_list);

		executor_send(executor, dtr, 0, &req_list);
		executor_recv(executor, dtr, 0);
	);
	Buf* error = NULL;
	if (on_error)
		error = msg_error(&am_self()->error);

	executor_commit(executor, dtr, error);
}

hot static void
on_write(Primary* self, Buf* data)
{
	Session* session = self->replay_arg;

	// take shared lock
	session_lock(session, LOCK);
	defer(session_unlock, session);

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
			session_lock(session, LOCK_EXCLUSIVE);

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

	Recover recover;
	recover_init(&recover, share->db, &build_if, share->cluster);
	defer(recover_free, &recover);

	Primary primary;
	primary_init(&primary, &recover, self->client, on_write, self);
	primary_main(&primary);
}
