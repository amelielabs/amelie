
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
