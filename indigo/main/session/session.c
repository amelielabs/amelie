
//
// indigo
//	
// SQL OLTP database
//

#include <indigo_runtime.h>
#include <indigo_io.h>
#include <indigo_data.h>
#include <indigo_lib.h>
#include <indigo_config.h>
#include <indigo_auth.h>
#include <indigo_client.h>
#include <indigo_server.h>
#include <indigo_def.h>
#include <indigo_transaction.h>
#include <indigo_index.h>
#include <indigo_storage.h>
#include <indigo_wal.h>
#include <indigo_db.h>
#include <indigo_value.h>
#include <indigo_aggr.h>
#include <indigo_request.h>
#include <indigo_vm.h>
#include <indigo_parser.h>
#include <indigo_semantic.h>
#include <indigo_compiler.h>
#include <indigo_shard.h>
#include <indigo_hub.h>
#include <indigo_session.h>

Session*
session_create(Share* share, Portal* portal)
{
	auto self = (Session*)in_malloc(sizeof(Session));
	self->lock   = NULL;
	self->share  = share;
	self->portal = portal;
	vm_init(&self->vm, share->db, share->function_mgr, NULL);
	compiler_init(&self->compiler, share->db, share->function_mgr,
	              share->router, &self->dispatch);
	command_init(&self->cmd);
	explain_init(&self->explain);
	dispatch_init(&self->dispatch, share->dispatch_lock,
	               share->req_cache,
	               share->router,
	               &self->compiler.code_data);
	log_set_init(&self->log_set);
	return self;
}

void
session_free(Session *self)
{
	assert(! self->lock);
	vm_free(&self->vm);
	compiler_free(&self->compiler);
	command_free(&self->cmd);
	dispatch_reset(&self->dispatch);
	dispatch_free(&self->dispatch);
	log_set_free(&self->log_set);
	in_free(self);
}

static inline void
session_reset(Session* self)
{
	vm_reset(&self->vm);
	compiler_reset(&self->compiler);
	explain_reset(&self->explain);
	dispatch_reset(&self->dispatch);
	log_set_reset(&self->log_set);
	palloc_truncate(0);
}

static inline void
session_lock(Session* self)
{
	// take shared session lock
	self->lock = lock_lock(self->share->session_lock, true);
}

static inline void
session_unlock(Session* self)
{
	if (! self->lock)
		return;
	lock_unlock(self->lock);
	self->lock = NULL;
}

hot static inline void
session_explain(Session* self)
{
	// todo: profile

	auto buf = explain(&self->explain,
	                   &self->compiler.code_coordinator,
	                   &self->compiler.code_data,
	                   &self->dispatch,
	                   false);

	portal_write(self->portal, buf);
}

hot static inline void
session_execute_distributed(Session* self)
{
	auto dispatch = &self->dispatch;
	auto log_set  = &self->log_set;

	// shared session lock
	session_lock(self);

	// todo: reference lock

	// generate request
	compiler_emit(&self->compiler);

	// explain
	if (self->compiler.parser.explain != EXPLAIN_NONE)
	{
		session_explain(self);
		return;
	}

	// send for shard execution
	dispatch_send(dispatch);

	Exception e;
	if (try(&e))
	{
		// execute coordinator
		vm_run(&self->vm, NULL,
		       &self->dispatch,
		       &self->cmd,
		       &self->compiler.code_coordinator,
		       &self->compiler.code_data,
		       NULL,
		       self->portal);

		// add logs to the log set
		dispatch_export(dispatch, log_set);

		// wal write
		uint64_t lsn = 0;
		if (log_set->count > 0)
		{
			lsn = config_lsn_next();

			//wal_write(share->wal, log_set);
			//lsn = log_set->lsn;
		}

		// send COMMIT
		dispatch_commit(dispatch, lsn);
	}

	if (catch(&e))
	{
		// send ABORT and rethrow
		dispatch_abort(dispatch);
		rethrow();
	}
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
		// execute utility or DDL command by system
		auto stmt = compiler_first(&self->compiler);
		Buf* buf = NULL;
		rpc(global()->control->system, RPC_CTL, 3, self, stmt, &buf);
		if (buf)
			portal_write(self->portal, buf);
	} else
	{
		// DML or Select
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
	}

	bool ro = false;
	if (catch(&e))
	{
		session_unlock(self);

		auto error = &in_self()->error;
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
