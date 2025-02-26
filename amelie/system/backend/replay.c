
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
#include <amelie_column.h>
#include <amelie_heap.h>
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
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
replay_read(Share* share, Dtr* dtr, ReqList* req_list, Record* record)
{
	auto router = &share->backend_mgr->router;

	// redistribute rows between backends
	Req* map[router->list_count];
	memset(map, 0, sizeof(map));

	// replay transaction log record
	auto cmd = record_cmd(record);
	auto pos = record_data(record);
	for (auto i = record->count; i > 0; i--)
	{
		// map each write to route
		auto table_mgr = &share->db->table_mgr;
		auto part = table_mgr_find_partition(table_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu64, cmd->partition);
		auto route = part->route;
		auto req = map[route->order];
		if (req == NULL)
		{
			req = req_create(&dtr->req_cache, REQ_REPLAY);
			req->route = route;
			req_list_add(req_list, req);
			map[route->order] = req;
		}

		// [cmd, pos]
		buf_write(&req->arg, (uint8_t*)&cmd, sizeof(uint8_t**));
		buf_write(&req->arg, (uint8_t*)&pos, sizeof(uint8_t**));

		pos += cmd->size;
		cmd++;
	}
}

void
replay(Share* share, Dtr* dtr, Record* record)
{
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

	auto executor = share->executor;
	auto on_error = error_catch
	(
		replay_read(share, dtr, &req_list, record);

		executor_send(executor, dtr, 0, &req_list);
		executor_recv(executor, dtr, 0);
	);
	Buf* error = NULL;
	if (on_error)
		error = msg_error(&am_self()->error);

	executor_commit(executor, dtr, error);
}
