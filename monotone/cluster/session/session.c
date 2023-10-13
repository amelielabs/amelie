
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
#include <monotone_transaction.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_vm.h>
#include <monotone_parser.h>
#include <monotone_compiler.h>
#include <monotone_shard.h>
#include <monotone_session.h>

void
session_init(Session* self, Share* share, Portal* portal)
{
	self->lock        = LOCK_NONE;
	self->lock_shared = NULL;
	self->share       = share;
	self->portal      = portal;
	request_lock_init(&self->lock_req);
	compiler_init(&self->compiler, share->db);
	command_init(&self->cmd);
	transaction_init(&self->trx);
	request_set_init(&self->req_set, share->req_cache);
	log_set_init(&self->log_set);
}

void
session_free(Session *self)
{
	assert(self->lock == LOCK_NONE);
	compiler_free(&self->compiler);
	command_free(&self->cmd);
	request_set_reset(&self->req_set);
	request_set_free(&self->req_set);
	log_set_free(&self->log_set);
	transaction_free(&self->trx);
}

static inline void
session_reset(Session* self)
{
	auto share = self->share;
	palloc_truncate(0);
	if (likely(request_set_created(&self->req_set)))
		request_set_reset(&self->req_set);
	else
		request_set_create(&self->req_set, share->shard_mgr->shards_count);
	log_set_reset(&self->log_set);
	compiler_reset(&self->compiler);
}

static Code*
execute_add(uint32_t hash, void* arg)
{
	Session* self = arg;

	// get shard by hash
	auto shard = shard_map_get(self->share->shard_map, hash);

	// map request to the shard
	auto req = request_set_add(&self->req_set, shard->order, &shard->task.channel);
	return &req->code;
}

hot static inline void
session_execute_distributed(Session* self)
{
	auto share = self->share;
	auto req_set = &self->req_set;
	auto log_set = &self->log_set;

	// lock catalog
	session_lock(self, LOCK_SHARED);

	// generate request
	compiler_generate(&self->compiler, execute_add, self);
	request_set_finilize(req_set);

	// execute
	request_sched_lock(share->req_sched);
	request_set_execute(req_set);
	request_sched_unlock(share->req_sched);

	// wait for completion
	request_set_wait(req_set);

	// commit (no errors)
	if (likely(! req_set->error))
	{
		// add logs to the log set
		request_set_commit_prepare(req_set, log_set);

		// wal write
		uint64_t lsn = 0;
		if (log_set->count > 0)
		{
			//wal_write(share->wal, log_set);
			//lsn = log_set->lsn;
		}

		// send COMMIT
		request_set_commit(req_set, lsn);
		return;
	}

	// send ABORT and throw error
	request_set_abort(req_set);
}

hot static inline void
session_main(Session* self, Buf* buf)
{
	// read command
	command_set(&self->cmd, buf);

	// parse query
	compiler_parse(&self->compiler, &self->cmd.text);

	if (compiler_is_utility(&self->compiler))
	{
		// DDL or system command
		session_execute_utility(self);
	} else
	{
		// DML and Select
		session_execute_distributed(self);
	}

	session_unlock(self);
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

		session_reset(self);
		session_main(self, buf);
		palloc_truncate(0);
	}

	bool ro = false;
	if (catch(&e))
	{
		session_unlock(self);

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
