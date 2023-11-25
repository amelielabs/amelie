
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
#include <monotone_def.h>
#include <monotone_transaction.h>
#include <monotone_snapshot.h>
#include <monotone_storage.h>
#include <monotone_wal.h>
#include <monotone_db.h>
#include <monotone_value.h>
#include <monotone_aggr.h>
#include <monotone_request.h>

void
dispatch_init(Dispatch* self, DispatchLock* lock,
              ReqCache* cache,
              Router*   router,
              CodeData* code_data)
{
	self->set          = NULL;
	self->set_size     = 0;
	self->stmt_count   = 0;
	self->stmt_current = 0;
	self->error        = NULL;
	self->code_data    = code_data;
	self->router       = router;
	self->cache        = cache;
	self->lock         = lock;
	buf_init(&self->stmt);
}

void
dispatch_free(Dispatch* self)
{
	if (self->set)
	{
		for (int i = 0; i < self->set_size; i++)
		{
			auto req = self->set[i];
			if (req)
				req_cache_push(self->cache, req);
		}
		mn_free(self->set);
	}
	if (self->error)
		buf_free(self->error);
	buf_free(&self->stmt);
}

void
dispatch_reset(Dispatch* self)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (req)
		{
			req_cache_push(self->cache, req);
			self->set[i] = NULL;
		}
	}
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->stmt_count   = 0;
	self->stmt_current = 0;
	buf_reset(&self->stmt);
}

void
dispatch_prepare(Dispatch* self, int stmts)
{
	// prepare request set
	if (unlikely(self->set == NULL))
	{
		int size = sizeof(Req*) * self->router->set_size;
		self->set = mn_malloc(size);
		self->set_size = self->router->set_size;
		memset(self->set, 0, size);
	}

	// [stmts]
	//         0   1   n  [cores]
	// 0       x       x
	// 1           x
	// n       x   x   x
	//

	// prepare request set per stmt set as [statements][routes]
	int size = sizeof(Req*) * stmts * self->set_size;
	buf_reserve(&self->stmt, size);
	auto start = self->stmt.position;
	memset(start, 0, size);
	buf_advance(&self->stmt, size);
	self->stmt_count = stmts;
}

hot Req*
dispatch_add(Dispatch* self, int stmt, Route* route)
{
	// add request per route
	auto req = self->set[route->order];
	if (! req)
	{
		req = req_create(self->cache);
		req->dst = route->channel;
		req->code_data = self->code_data;
		result_prepare(&req->result, self->stmt_count);
		self->set[route->order] = req;
	}

	// set request per statement
	auto index = (Req**)(self->stmt.start);
	index[stmt * self->set_size + route->order] = req;
	return req;
}

hot void
dispatch_export(Dispatch* self, LogSet* set)
{
	for (int i = 0; i < self->set_size; i++)
	{
		auto req = self->set[i];
		if (!req || !req->trx.log.count_persistent)
			continue;
		log_set_add(set, &req->trx.log);
	}
}
