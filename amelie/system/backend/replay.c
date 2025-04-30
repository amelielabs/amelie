
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>
#include <amelie_func.h>
#include <amelie_vm.h>
#include <amelie_parser.h>
#include <amelie_compiler.h>
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
replay_read(Share* share, Dtr* dtr, ReqList* req_list, Record* record)
{
	// redistribute rows between backends
	Req* map[dtr->core_mgr->cores_count];
	memset(map, 0, sizeof(map));

	// replay transaction log record
	auto cmd = record_cmd(record);
	auto pos = record_data(record);
	for (auto i = record->count; i > 0; i--)
	{
		// map each write to route
		auto part = part_mgr_find(&share->db->part_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu64, cmd->partition);
		auto core = part->core;
		auto req = map[core->order];
		if (req == NULL)
		{
			req = req_create(&dtr->dispatch_mgr.req_cache);
			req->type = REQ_REPLAY;
			req->core = core;
			req_list_add(req_list, req);
			map[core->order] = req;
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
	ReqList req_list;
	req_list_init(&req_list);

	auto executor = share->executor;
	auto on_error = error_catch
	(
		replay_read(share, dtr, &req_list, record);

		executor_send(executor, dtr, &req_list);
		executor_recv(executor, dtr);
	);
	Buf* error = NULL;
	if (on_error)
	{
		error = msg_error(&am_self()->error);
		req_cache_push_list(&dtr->dispatch_mgr.req_cache, &req_list);
	}

	executor_commit(executor, dtr, error);
}
