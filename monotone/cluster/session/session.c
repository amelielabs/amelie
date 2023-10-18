
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
#include <monotone_request.h>
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
	cat_lock_init(&self->lock_req);
	vm_init(&self->coordinator, share->db, share->function_mgr, NULL);
	compiler_init(&self->compiler, share->db, share->router, &self->dispatch);
	command_init(&self->cmd);
	transaction_init(&self->trx);
	dispatch_init(&self->dispatch, share->dispatch_lock,
	               share->req_cache,
	               share->router,
	               &self->compiler.code_data);
	log_set_init(&self->log_set);
}

void
session_free(Session *self)
{
	assert(self->lock == LOCK_NONE);
	vm_free(&self->coordinator);
	compiler_free(&self->compiler);
	command_free(&self->cmd);
	dispatch_reset(&self->dispatch);
	dispatch_free(&self->dispatch);
	log_set_free(&self->log_set);
	transaction_free(&self->trx);
}

static inline void
session_reset(Session* self)
{
	palloc_truncate(0);
	vm_reset(&self->coordinator);
	compiler_reset(&self->compiler);
	dispatch_reset(&self->dispatch);
	log_set_reset(&self->log_set);
}

hot static inline void
session_execute_distributed(Session* self)
{
	auto dispatch = &self->dispatch;
	auto log_set = &self->log_set;

	// lock catalog
	session_lock(self, LOCK_SHARED);

	// generate request
	compiler_emit(&self->compiler);

	// send for shard execution
	dispatch_send(dispatch);

	Exception e;
	if (try(&e))
	{
		// execute coordinator
		vm_run(&self->coordinator, &self->trx, &self->dispatch,
		       &self->cmd,
		       &self->compiler.code_coordinator,
		       &self->compiler.code_data,
		        0, NULL, self->portal);

		// add logs to the log set
		dispatch_export(dispatch, log_set);

		// wal write
		uint64_t lsn = 0;
		if (log_set->count > 0)
		{
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
		// DDL or system command
		session_execute_utility(self);
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
