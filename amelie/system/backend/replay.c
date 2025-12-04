
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
#include <amelie_storage>
#include <amelie_repl>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
replay_read(Dtr* dtr, Dispatch* dispatch, Record* record)
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
		auto part = part_mgr_find(&share()->storage->part_mgr, cmd->partition);
		if (! part)
			error("failed to find partition %" PRIu32, cmd->partition);
		auto core = part->core;
		auto req = map[core->order];
		if (req == NULL)
		{
			req = dispatch_add(dispatch, &dtr->dispatch_mgr.cache_req,
			                   REQ_REPLAY, -1, NULL, NULL,
			                   core);
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
replay(Dtr* dtr, Record* record)
{
	auto dispatch_mgr = &dtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	dispatch_set_close(dispatch);

	auto on_error = error_catch (
		replay_read(dtr, dispatch, record);
	);
	if (on_error)
	{
		dispatch_reset(dispatch, &dispatch_mgr->cache_req);
		dispatch_free(dispatch);
		rethrow();
	}

	executor_send(share()->executor, dtr, dispatch);
	commit(share()->commit, dtr, NULL);
}
