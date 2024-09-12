#pragma once

//
// amelie.
//
// Real-Time SQL Database.
//

typedef struct Dtr Dtr;

typedef enum
{
	DTR_NONE,
	DTR_BEGIN,
	DTR_PREPARE,
	DTR_ERROR,
	DTR_COMMIT,
	DTR_ABORT
} DtrState;

struct Dtr
{
	DtrState   state;
	PipeSet    set;
	Dispatch   dispatch;
	bool       snapshot;
	bool       repl;
	Code*      code;
	CodeData*  code_data;
	Result     cte;
	Buf*       error;
	Cond       on_commit;
	Limit      limit;
	PipeCache  pipe_cache;
	ReqCache   req_cache;
	Router*    router;
	Local*     local;
	List       link_commit;
	List       link;
};

static inline void
dtr_init(Dtr* self, Router* router)
{
	self->state     = DTR_NONE;
	self->snapshot  = false;
	self->repl      = false;
	self->code      = NULL;
	self->code_data = NULL;
	self->error     = NULL;
	self->router    = router;
	self->local     = NULL;
	cond_init(&self->on_commit);
	pipe_set_init(&self->set);
	dispatch_init(&self->dispatch);
	result_init(&self->cte);
	limit_init(&self->limit, var_int_of(&config()->limit_write));
	pipe_cache_init(&self->pipe_cache);
	req_cache_init(&self->req_cache);
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_reset(Dtr* self)
{
	dispatch_reset(&self->dispatch,& self->req_cache);
	pipe_set_reset(&self->set, &self->pipe_cache);
	result_reset(&self->cte);
	limit_reset(&self->limit, var_int_of(&config()->limit_write));
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->state    = DTR_NONE;
	self->snapshot = false;
	self->local    = NULL;
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_free(&self->dispatch);
	pipe_set_free(&self->set);
	pipe_cache_free(&self->pipe_cache);
	req_cache_free(&self->req_cache);
	result_free(&self->cte);
	cond_free(&self->on_commit);
}

static inline void
dtr_create(Dtr*      self,
           Local*    local,
           Code*     code,
           CodeData* code_data,
           int       stmt_count,
           int       stmt_last)
{
	auto set_size = self->router->list_count;
	self->code      = code;
	self->code_data = code_data;
	self->local     = local;
	pipe_set_create(&self->set, set_size);
	dispatch_create(&self->dispatch, set_size, stmt_count, stmt_last);
	result_create(&self->cte, stmt_count);
}

static inline void
dtr_set_state(Dtr* self, DtrState state)
{
	self->state = state;
}

static inline void
dtr_set_snapshot(Dtr* self)
{
	self->snapshot = true;
}

static inline void
dtr_set_repl(Dtr* self)
{
	self->repl = true;
}

static inline void
dtr_set_error(Dtr* self, Buf* buf)
{
	assert(! self->error);
	self->error = buf;
}

static inline void
dtr_shutdown(Dtr* self)
{
	auto set = &self->set;
	for (int i = 0; i < set->set_size; i++)
	{
		auto pipe = pipe_set_get(set, i);
		if (pipe)
			req_queue_shutdown(&pipe->queue);
	}
}

hot static inline void
dtr_send(Dtr* self, int stmt, ReqList* list)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;

	// first send with snapshot
	if (self->snapshot && !self->dispatch.sent)
	{
		// create pipes for all nodes
		list_foreach(&self->router->list)
		{
			auto route = list_at(Route, link);
			auto pipe = pipe_create(&self->pipe_cache, route);
			pipe_set_set(set, route->order, pipe);
		}
	}

	// process request list
	list_foreach_safe(&list->list)
	{
		auto req   = list_at(Req, link);
		auto route = req->route;
		auto pipe  = pipe_set_get(set, route->order);
		if (! pipe)
		{
			pipe = pipe_create(&self->pipe_cache, route);
			pipe_set_set(set, route->order, pipe);
		}
		req_set(req, self->local,
		        self->code,
		        self->code_data,
		        &self->cte,
		        &self->limit);

		// set pipe per statement node order and add request to the
		// statement list
		auto ref = dispatch_pipe_set(dispatch, stmt, route->order, pipe);
		req_list_move(list, &ref->req_list, req);

		// add request to the pipe
		pipe_send(pipe, req);
		dispatch->sent++;
	}

	// shutdown pipes
	if (stmt == dispatch->stmt_last)
	{
		list_foreach(&self->router->list)
		{
			auto route = list_at(Route, link);
			auto pipe  = pipe_set_get(set, route->order);
			if (! pipe)
				continue;

			// add dummy request to the pipe to sync with recv, if pipe was only
			// used for snapshotting
			if (! pipe->sent)
			{
				auto ref = dispatch_pipe_set(dispatch, stmt, route->order, pipe);
				Req* req = req_create(&self->req_cache);
				req_list_add(&ref->req_list, req);
				pipe_send(pipe, req);
				dispatch->sent++;
			}

			pipe_shutdown(pipe);
		}
	}
}

hot static inline void
dtr_recv(Dtr* self, int stmt)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;

	Buf* error = NULL;
	for (int i = 0; i < set->set_size; i++)
	{
		auto pipe = dispatch_pipe(dispatch, stmt, i);
		if (! pipe)
			continue;
		auto ptr = pipe_recv(pipe);
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
		guard_buf(error);
		msg_error_rethrow(error);
	}
}
