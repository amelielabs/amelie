
//
// monotone
//	
// SQL OLTP database
//

#include <monotone_runtime.h>
#include <monotone_io.h>
#include <monotone_data.h>
#include <monotone_lib.h>
#include <monotone_config.h>
#include <monotone_auth.h>
#include <monotone_client.h>
#include <monotone_server.h>
#include <monotone_schema.h>
#include <monotone_mvcc.h>
#include <monotone_engine.h>
#include <monotone_storage.h>
#include <monotone_db.h>
#include <monotone_shard.h>
#include <monotone_session.h>

void
session_init(Session* self, ShardMgr* shard_mgr, RequestSched* req_sched,
             Portal* portal)
{
	self->shard_mgr = shard_mgr;
	self->req_sched = req_sched;
	self->portal    = portal;
	request_set_init(&self->req_set);
	request_cache_init(&self->req_cache);
}

void
session_free(Session *self)
{
	request_set_reset(&self->req_set, &self->req_cache);
	request_set_free(&self->req_set);
	request_cache_free(&self->req_cache);
}

hot static inline void
execute(Session* self, Buf* buf)
{
	unused(buf);

	request_set_reset(&self->req_set, &self->req_cache);

	for (int i = 0; i < self->shard_mgr->shards_count; i++)
	{
		auto shard = self->shard_mgr->shards[i];
		auto req = request_create(&self->req_cache, &shard->task.channel);
		req->ro = false;
		request_set_add(&self->req_set, req);
	}

	// execute

	// todo: lock

	request_sched_lock(self->req_sched);
	request_set_execute(&self->req_set);
	request_sched_unlock(self->req_sched);

	// wait for completion
	request_set_wait(&self->req_set);

	// commit

	// todo: signal for completion

	// rw

	{
		list_foreach(&self->req_set.list)
		{
			auto req = list_at(Request, link);
			assert(req->on_commit);
			condition_signal(req->on_commit);
		}
	}
}

hot bool
session_execute(Session* self, Buf* buf)
{
	Buf* reply;

	Exception e;
	if (try(&e))
	{
		auto msg = msg_of(buf);
		if (unlikely(msg->id != MSG_COMMAND))
			error("unrecognized request: %d", msg->id);

		execute(self, buf);
	}

	bool ro = false;
	if (catch(&e))
	{
		auto error = &mn_self()->error;
		reply = make_error(error);
		portal_write(self->portal, reply);
		ro = (error->code == ERROR_RO);
	}

	// ready
	reply = msg_create(MSG_OK);
	encode_integer(reply, false);
	msg_end(reply);
	portal_write(self->portal, reply);
	return ro;
}
