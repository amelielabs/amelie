
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
#include <amelie_sync>
#include <amelie_vm>
#include <amelie_frontend.h>
#include <amelie_backend.h>

static void
replay_read(Gtr* gtr, Dispatch* dispatch, Record* record)
{
	auto db = share()->db;
	Req* last = NULL;

	// replay transaction log record
	auto cmd = record_cmd(record);
	auto pos = record_data(record);
	for (auto i = record->count; i > 0; i--)
	{
		switch (cmd->cmd) {
		case CMD_PUBLISH:
		{
			// find topic
			auto rel   = catalog_find_by(&db->catalog, REL_TOPIC, &cmd->id, true);
			auto topic = topic_of(rel);
			auto size  = data_sizeof(pos);
			publish(topic, &gtr->tr, pos, size);
			break;
		}
		case CMD_ACK:
		{
			// find and update subscription
			auto rel = catalog_find_by(&db->catalog, REL_SUBSCRIPTION, &cmd->id, true);
			auto sub = sub_of(rel);
			acknowledge(sub, &gtr->tr, pos);
			break;
		}
		case CMD_REPLACE:
		case CMD_DELETE:
		{
			// redistribute rows between partitions
			auto row = (Row*)pos;

			// sync tsn
			state_tsn_follow(row->tsn);

			// match partition
			auto rel = catalog_find_by(&db->catalog, REL_TABLE, &cmd->id, true);
			auto table = table_of(rel);
			auto part  = part_mapping_map(&table->part_mgr.mapping, row);
			if (! part)
				error("replay: failed to find partition");

			// prepare request
			Req* req = NULL;
			if (last && last->part == part)
				req = last;
			else
			{
				req = dispatch_find(dispatch, part);
				if (! req)
					req = dispatch_add(dispatch, &gtr->dispatch_mgr.cache_req,
					                   REQ_REPLAY, -1, NULL, NULL,
					                   part);
			}
			last = req;

			// [cmd, pos]
			buf_write(&req->arg, (uint8_t*)&cmd, sizeof(uint8_t**));
			buf_write(&req->arg, (uint8_t*)&pos, sizeof(uint8_t**));
			break;
		}
		}

		pos += cmd->size;
		cmd++;
	}
}

void
replay(Gtr* gtr, Record* record)
{
	auto dispatch_mgr = &gtr->dispatch_mgr;
	auto dispatch = dispatch_create(&dispatch_mgr->cache);
	dispatch_set_close(dispatch);

	auto on_error = error_catch (
		replay_read(gtr, dispatch, record);
	);
	if (on_error)
	{
		dispatch_reset(dispatch, &dispatch_mgr->cache_req);
		dispatch_free(dispatch);
		rethrow();
	}

	executor_send(share()->executor, gtr, dispatch);
	commit(share()->commit, gtr, NULL);
}
