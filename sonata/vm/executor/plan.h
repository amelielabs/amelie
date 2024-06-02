#pragma once

//
// sonata.
//
// Real-Time SQL Database.
//

typedef struct Plan Plan;

typedef enum
{
	PLAN_NONE,
	PLAN_ACTIVE,
	PLAN_COMPLETE,
	PLAN_COMMIT,
	PLAN_ABORT
} PlanState;

struct Plan
{
	PlanState  state;
	TrxSet     set;
	Dispatch   dispatch;
	bool       snapshot;
	Code*      code;
	CodeData*  code_data;
	Result     cte;
	Buf*       error;
	Condition* on_commit;
	TrxCache*  trx_cache;
	ReqCache*  req_cache;
	Router*    router;
	List       link_group;
	List       link;
};

static inline void
plan_init(Plan*     self, Router* router,
          TrxCache* trx_cache,
          ReqCache* req_cache)
{
	self->state     = PLAN_NONE;
	self->snapshot  = false;
	self->code      = NULL;
	self->code_data = NULL;
	self->error     = NULL;
	self->on_commit = NULL;
	self->req_cache = req_cache;
	self->trx_cache = trx_cache;
	self->router    = router;
	trx_set_init(&self->set);
	dispatch_init(&self->dispatch);
	result_init(&self->cte);
	list_init(&self->link_group);
	list_init(&self->link);
}

static inline void
plan_free(Plan* self)
{
	dispatch_reset(&self->dispatch, self->req_cache);
	dispatch_free(&self->dispatch);
	trx_set_free(&self->set);
	result_free(&self->cte);
	if (self->error)
		buf_free(self->error);
	if (self->on_commit)
		condition_free(self->on_commit);
}

static inline void
plan_reset(Plan* self)
{
	dispatch_reset(&self->dispatch, self->req_cache);
	trx_set_reset(&self->set);
	result_reset(&self->cte);
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->state    = PLAN_NONE;
	self->snapshot = false;
}

static inline void
plan_create(Plan* self, Code* code, CodeData* code_data,
            int   stmt_count,
            int   stmt_last)
{
	auto set_size = self->router->list_count;
	self->code = code;
	self->code_data = code_data;
	trx_set_create(&self->set, set_size);
	dispatch_create(&self->dispatch, set_size, stmt_count, stmt_last);
	result_create(&self->cte, stmt_count);
	if (! self->on_commit)
		self->on_commit = condition_create();
}

static inline void
plan_set_state(Plan* self, PlanState state)
{
	self->state = state;
}

static inline void
plan_set_snapshot(Plan* self)
{
	self->snapshot = true;
}

static inline void
plan_set_error(Plan* self, Buf* buf)
{
	assert(! self->error);
	self->error = buf;
}

static inline void
plan_shutdown(Plan* self)
{
	auto set = &self->set;
	for (int i = 0; i < set->set_size; i++)
	{
		auto trx = trx_set_get(set, i);
		if (trx)
			req_queue_shutdown(&trx->queue);
	}
}

hot static inline void
plan_send(Plan* self, int stmt, ReqList* list)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;

	// first send with snapshot
	if (self->snapshot && self->state == PLAN_NONE)
	{
		// create transactions for all nodes
		list_foreach(&self->router->list)
		{
			auto route = list_at(Route, link);
			auto trx = trx_create(self->trx_cache);
			trx_set(trx, route, self->code, self->code_data, &self->cte);
			trx_set_set(set, route->order, trx);
		}
	}

	// process request list
	bool is_last = (stmt == dispatch->stmt_last);
	list_foreach_safe(&list->list)
	{
		auto req   = list_at(Req, link);
		auto route = req->route;
		auto trx   = trx_set_get(set, route->order);
		if (trx == NULL)
		{
			trx = trx_create(self->trx_cache);
			trx_set(trx, route, self->code, self->code_data, &self->cte);
			trx_set_set(set, route->order, trx);
		}

		// set transaction per statement node order and add request to the
		// statement list
		auto ref = dispatch_trx_set(dispatch, stmt, route->order, trx);
		req_list_move(list, &ref->req_list, req);

		// add request to the transaction queue for execution
		trx_send(trx, req, is_last);
	}

	// first send with snapshot
	if (self->snapshot && is_last)
	{
		// shutdown queue (and transaction) if it was only used for
		// snapshotting
		plan_shutdown(self);	
	}
}

hot static inline void
plan_recv(Plan* self, int stmt)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;

	Buf* error = NULL;
	for (int i = 0; i < set->set_size; i++)
	{
		auto trx = dispatch_trx(dispatch, stmt, i);
		if (trx == NULL)
			continue;
		auto ptr = trx_recv(trx);
		if (unlikely(ptr))
		{
			if (! error)
				error = ptr;
			else
				buf_free(ptr);
		}
	}

	if (unlikely(error))
	{
		// force to complete active transactions
		plan_shutdown(self);	

		guard_buf(error);
		msg_error_rethrow(error);
	}
}
