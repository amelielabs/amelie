
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

hot void
dispatch_commit(Dispatch* self, uint64_t lsn)
{
	for (int order = 0; order < self->set_size; order++)
	{
		auto req = dispatch_at(self, order);
		if (! req)
			continue;

		// set transaction lsn
		transaction_set_lsn(&req->trx, lsn);

		// RPC_REQUEST_COMMIT
		auto buf = msg_create(RPC_REQUEST_COMMIT);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);
		channel_write(req->dst, buf);

		// done
		self->set[order] = NULL;
	}
}

void
dispatch_abort(Dispatch* self)
{
	for (int order = 0; order < self->set_size; order++)
	{
		auto req = dispatch_at(self, order);
		if (! req)
			continue;

		// done
		self->set[order] = NULL;

		// already aborted
		if (req->error)
		{
			// put req back to the cache
			req_cache_push(req->cache, req);
			continue;
		}

		// RPC_REQUEST_ABORT
		auto buf = msg_create(RPC_REQUEST_ABORT);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);
		channel_write(req->dst, buf);
	}
}

hot void
dispatch_send(Dispatch* self)
{
	dispatch_lock(self->lock);
	guard(unlock, dispatch_unlock, self->lock);

	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (! req)
			continue;

		auto buf = msg_create(RPC_REQUEST);
		buf_write(buf, &req, sizeof(void**));
		msg_end(buf);
		channel_write(req->dst, buf);
	}
}

hot static inline void
dispatch_read(Dispatch* self, Req* req, Portal* portal)
{
	// blocking wait next result
	auto buf = channel_read(&req->src, -1);
	assert(buf);

	auto msg = msg_of(buf);
	switch (msg->id) {
	case MSG_OBJECT:
		if (portal)
			portal_write(portal, buf);
		return;
	case MSG_READY:
		// set last completed statement
		req->stmt = self->stmt_current;
		break;
	case MSG_OK:
		req->stmt     = INT_MAX;
		req->complete = true;
		break;
	case MSG_ERROR:
		if (! self->error)
		{
			self->error = buf;
			return;
		}
		break;
	default:
		break;
	}

	buf_free(buf);
}

hot static void
dispatch_rethrow(Dispatch* self)
{
	// rethrow error message
	uint8_t* pos = msg_of(self->error)->data;
	int count;
	data_read_map(&pos, &count);
	// code
	data_skip(&pos);
	data_skip(&pos);
	// msg
	data_skip(&pos);
	Str text;
	data_read_string(&pos, &text);
	error("%.*s", str_size(&text), str_of(&text));
}

hot static inline void
dispatch_drain(Dispatch* self)
{
	for (;;)
	{
		int processed = 0;
		for (int order = 0; order < self->set_size; order++)
		{
			auto req = dispatch_at(self, order);
			if (!req || req->complete)
				continue;
			processed++;
			dispatch_read(self, req, NULL);
		}

		if (! processed)
			break;
	}

	for (int order = 0; order < self->set_size; order++)
	{
		auto req = dispatch_at(self, order);
		if (! req)
			continue;
		result_reset(&req->result);
	}
}

hot void
dispatch_recv(Dispatch* self, Portal* portal)
{
	if (self->stmt_current == self->stmt_count)
		return;

	assert(! self->error);
	coroutine_cancel_pause(in_self());

	// process routes 1 by 1 to get deterministic result
	int stmt = self->stmt_current;
	for (;;)
	{
		int processed = 0;
		for (int order = 0; order < self->set_size; order++)
		{
			auto req = dispatch_at_stmt(self, stmt, order);
			if (!req || req->complete)
				continue;
			processed++;
			dispatch_read(self, req, portal);
		}

		if (! processed)
			break;
	}

	// complete stmt
	self->stmt_current++;
	coroutine_cancel_resume(in_self());

	if (unlikely(self->error))
	{
		// read and free all pending replies
		dispatch_drain(self);

		// read and rethrow error
		dispatch_rethrow(self);
	}
}
