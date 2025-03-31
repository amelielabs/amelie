
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
#include <amelie_value.h>
#include <amelie_set.h>
#include <amelie_content.h>
#include <amelie_executor.h>

void
executor_init(Executor* self, Db* db)
{
	self->seq           = 0;
	self->shutdown      = false;
	self->db            = db;
	self->list_count    = 0;
	self->pending_count = 0;
	commit_init(&self->commit);
	req_mgr_init(&self->req_mgr);
	list_init(&self->list);
	list_init(&self->pending);
	mutex_init(&self->lock);
	cond_var_init(&self->cond_var);
}

void
executor_free(Executor* self)
{
	mutex_free(&self->lock);
	cond_var_free(&self->cond_var);
	req_mgr_free(&self->req_mgr);
}

void
executor_shutdown(Executor* self)
{
	mutex_lock(&self->lock);
	self->shutdown = true;
	cond_var_broadcast(&self->cond_var);
	mutex_unlock(&self->lock);
}

hot void
executor_send(Executor* self, Dtr* dtr, int step, ReqList* list)
{
	mutex_lock(&self->lock);

	// add transaction to the executor list
	if (step == 0)
	{
		// assign transaction id
		dtr_set_state(dtr, DTR_BEGIN);
		dtr->id = ++self->seq;

		list_append(&self->list, &dtr->link);
		self->list_count++;

		// TODO wait for exclusive lock
	}

	bool wakeup = false;
	list_foreach_safe(&list->list)
	{
		auto req = list_at(Req, link);
		req->arg.dtr  = dtr;
		req->arg.step = step;

		// move req to the dispatch step list
		req_list_del(list, req);
		dispatch_add(&dtr->dispatch, step, req);

		// add req to the backlog and schedule backlog
		if (backlog_add(req->arg.backlog, &req->arg.backlog_req))
		{
			list_append(&self->pending, &req->arg.backlog->link);
			self->pending_count++;
			wakeup = true;
		}
	}

	// wakeup next worker
	if (wakeup)
		cond_var_signal(&self->cond_var);

	mutex_unlock(&self->lock);

	// TODO: unlock waiters?
}

hot void
executor_recv(Executor* self, Dtr* dtr, int step)
{
	unused(self);
	// OK or ABORT
	dispatch_wait(&dtr->dispatch, step);
}

hot static void
executor_prepare(Executor* self, bool abort)
{
	auto commit = &self->commit;
	commit_reset(commit);

	if (unlikely(abort))
	{
		// abort all currently active transactions
		list_foreach(&self->list)
		{
			auto dtr = list_at(Dtr, link);
			commit_add(commit, dtr);
		}
	} else
	{
		// collect a list of prepared distributed transactions
		list_foreach(&self->list)
		{
			auto dtr = list_at(Dtr, link);
			if (dtr->state != DTR_PREPARE)
				break;
			commit_add(commit, dtr);
		}
	}
}

hot static void
executor_end(Executor* self, DtrState state)
{
	auto commit = &self->commit;

	// send COMMIT/ABORT (max prepared transaction id) for each
	// involved partition
	auto type = REQ_COMMIT;
	if (likely(state == DTR_ABORT))
	{
		executor_prepare(self, true);
		type = REQ_ABORT;
	}
	auto wakeup = false;
	list_foreach_safe(&commit->list_backlogs)
	{
		auto backlog = list_at(Backlog, link_commit);
		auto req  = req_create(&self->req_mgr);
		req->arg.type = type;
		req->arg.backlog = backlog;
		buf_write(&req->arg.arg, &commit->list_max, sizeof(commit->list_max));
		list_init(&backlog->link_commit);

		// add req to the backlog
		if (backlog_add(req->arg.backlog, &req->arg.backlog_req))
			wakeup = true;
	}

	// wakeup next worker
	if (wakeup)
		cond_var_signal(&self->cond_var);

	// finilize distributed transactions
	list_foreach(&commit->list)
	{
		auto dtr = list_at(Dtr, link_commit);

		// update state
		auto tr_state = dtr->state;
		dtr_set_state(dtr, state);

		// remove transaction from the executor
		list_unlink(&dtr->link);
		self->list_count--;

		// wakeup
		if (tr_state == DTR_PREPARE ||
		    tr_state == DTR_ERROR)
			event_signal(&dtr->on_commit);
	}

	// wakeup next commit leader, if any
	if (self->list_count > 0)
	{
		auto leader = container_of(self->list.next, Dtr, link);
		if (leader->state == DTR_PREPARE ||
		    leader->state == DTR_ERROR)
			event_signal(&leader->on_commit);
	}
}

hot static void
executor_wal_write(Executor* self)
{
	// prepare a list of a finilized wal records
	auto commit = &self->commit;
	auto write_list = &commit->write;
	list_foreach(&commit->list)
	{
		auto dtr   = list_at(Dtr, link_commit);
		auto write = &dtr->write;
		write_reset(write);
		write_begin(write);

		// TODO
#if 0
		for (int i = 0; i < set->set_size; i++)
		{
			auto pipe = pipe_set_get(set, i);
			if (pipe == NULL)
				continue;
			assert(pipe->state == PIPE_CLOSE);
			assert(pipe->tr);
			write_add(write, &pipe->tr->log.write_log);
		}
		if (write->header.count > 0)
		{
			// unless transaction is used for replication writer, respect
			// system read-only state
			if (!tr->program->repl && var_int_of(&state()->read_only))
				error("system is in read-only mode");
			write_list_add(write_list, write);
		}
#endif
	}

	// do atomical wal write
	wal_mgr_write(&self->db->wal_mgr, write_list);
}

hot void
executor_commit(Executor* self, Dtr* dtr, Buf* error)
{
	// read all pending replies
	dispatch_drain(&dtr->dispatch);

	for (;;)
	{
		mutex_lock(&self->lock);

		switch (dtr->state) {
		case DTR_COMMIT:
			// commited by leader
			mutex_unlock(&self->lock);
			return;
		case DTR_ABORT:
			// aborted by leader
			mutex_unlock(&self->lock);
			error("transaction conflict, abort");
			return;
		case DTR_NONE:
			// non-distributed transaction
			mutex_unlock(&self->lock);
			if (error)
			{
				dtr_set_error(dtr, error);
				msg_error_rethrow(error);
			}
			return;
		case DTR_BEGIN:
			if (unlikely(error))
			{
				dtr_set_state(dtr, DTR_ERROR);
				dtr_set_error(dtr, error);
			} else {
				dtr_set_state(dtr, DTR_PREPARE);
			}
			break;
		case DTR_PREPARE:
		case DTR_ERROR:
			// retry after wakeup
			break;
		default:
			abort();
			break;
		}

		// commit leader is the first transaction in the list
		if (! list_is_first(&self->list, &dtr->link))
		{
			// wait to become leader or wakeup by leader
			mutex_unlock(&self->lock);

			event_wait(&dtr->on_commit, -1);
			continue;
		}

		// handle leader error
		if (dtr->state == DTR_ERROR)
		{
			executor_end(self, DTR_ABORT);
			mutex_unlock(&self->lock);
			msg_error_throw(dtr->error);
		}

		// prepare for group commit and wal write

		// get a list of completed distributed transactions (one or more) and
		// a list of last executed transactions per partition
		executor_prepare(self, false);
		mutex_unlock(&self->lock);

		// wal write
		DtrState state = DTR_COMMIT;
		if (unlikely(error_catch( executor_wal_write(self) )))
			state = DTR_ABORT;

		// process COMMIT or ABORT
		mutex_lock(&self->lock);
		executor_end(self, state);
		mutex_unlock(&self->lock);
		if (state == DTR_ABORT)
			rethrow();

		break;
	}
}

Req*
executor_next(Executor* self)
{
	Req* req = NULL;
	mutex_lock(&self->lock);
	for (;;)
	{
		if (self->pending_count > 0)
		{
			auto first = list_pop(&self->pending);
			self->pending_count--;
			auto backlog = container_of(first, Backlog, link);
			auto backlog_req = backlog_begin(backlog);
			assert(backlog_req);
			req = container_of(backlog_req, Req, arg.backlog_req);
			break;
		}
		if (self->shutdown)
			break;
		cond_var_wait(&self->cond_var, &self->lock);
	}
	mutex_unlock(&self->lock);
	return req;
}

void
executor_complete(Executor* self, Req* req)
{
	// send OK or ERROR based on the req result
	if (req->arg.dtr)
	{
		int id;
		if (req->arg.error)
			id = MSG_ERROR;
		else
			id = MSG_OK;
		auto buf = msg_create(id);
		msg_end(buf);
		auto step = dispatch_at(&req->arg.dtr->dispatch, req->arg.step);
		channel_write(&step->src, buf);
	}

	// finilize req and reschedule backlog
	mutex_lock(&self->lock);

	if (backlog_end(req->arg.backlog, &req->arg.backlog_req))
	{
		list_append(&self->pending, &req->arg.backlog->link);
		self->pending_count++;
		cond_var_signal(&self->cond_var);
	}

	mutex_unlock(&self->lock);
}
