
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

hot static inline Req*
dispatch_add(Dispatch* self, int stmt, Route* route)
{
	// prepare request set
	if (unlikely(self->set == NULL))
	{
		int size = sizeof(Req) * self->router->set_size;
		self->set = mn_malloc(size);
		self->set_size = self->router->set_size;
		memset(self->set, 0, size);
	}

	// add request per route
	auto req = self->set[route->order];
	if (! req)
	{
		req = req_create(self->cache);
		req->dst = route->channel;
		req->code_data = self->code_data;
		self->set[route->order] = req;
	}

	// add request statement as [statement][route]
	if (stmt == self->stmt_count)
	{
		int size = sizeof(Req*) * self->set_size;
		buf_reserve(&self->stmt, size);
		auto start = self->stmt.position;
		memset(start, 0, size);
		buf_advance(&self->stmt, size);
		self->stmt_count++;
	}

	auto index = (Req**)(self->stmt.start);
	index[stmt * self->set_size + route->order] = req;
	return req;
}

hot Req*
dispatch_map(Dispatch* self, uint32_t hash, int stmt)
{
	// get route by hash
	auto route = router_get(self->router, hash);

	// add request to the route
	return dispatch_add(self, stmt, route);
}

hot void
dispatch_copy(Dispatch* self, Code* code, int stmt)
{
	auto router = self->router;
	for (int i = 0; i < router->set_size; i++)
	{
		// get route by order
		auto route = router_at(router, i);

		// add request to the route
		auto req = dispatch_add(self, stmt, route);

		// append code
		code_copy(&req->code, code);
	}
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
