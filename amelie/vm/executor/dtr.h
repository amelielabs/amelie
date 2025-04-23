#pragma once

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
	DtrState  state;
	PipeSet   set;
	Dispatch  dispatch;
	Program*  program;
	Buf*      args;
	Result    cte;
	Buf*      error;
	Write     write;
	Event     on_access;
	Event     on_commit;
	Limit     limit;
	PipeCache pipe_cache;
	ReqCache  req_cache;
	Router*   router;
	Local*    local;
	List      link_prepare;
	List      link_access;
	List      link;
};

static inline void
dtr_init(Dtr* self, Router* router, Local* local)
{
	self->state   = DTR_NONE;
	self->program = NULL;
	self->args    = NULL;
	self->error   = NULL;
	self->router  = router;
	self->local   = local;
	pipe_set_init(&self->set);
	dispatch_init(&self->dispatch);
	result_init(&self->cte);
	event_init(&self->on_access);
	event_init(&self->on_commit);
	limit_init(&self->limit, opt_int_of(&config()->limit_write));
	write_init(&self->write);
	pipe_cache_init(&self->pipe_cache);
	req_cache_init(&self->req_cache);
	list_init(&self->link_prepare);
	list_init(&self->link_access);
	list_init(&self->link);
}

static inline void
dtr_reset(Dtr* self)
{
	dispatch_reset(&self->dispatch,& self->req_cache);
	pipe_set_reset(&self->set, &self->pipe_cache);
	result_reset(&self->cte);
	limit_reset(&self->limit, opt_int_of(&config()->limit_write));
	write_reset(&self->write);
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->state   = DTR_NONE;
	self->program = NULL;
	self->args    = NULL;
	list_init(&self->link_prepare);
	list_init(&self->link_access);
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
	event_detach(&self->on_access);
	event_detach(&self->on_commit);
	write_free(&self->write);
}

static inline void
dtr_create(Dtr* self, Program* program, Buf* args)
{
	self->program = program;
	self->args    = args;
	auto set_size = self->router->list_count;
	pipe_set_create(&self->set, set_size);
	dispatch_create(&self->dispatch, set_size, program->stmts,
	                program->stmts_last);
	result_create(&self->cte, program->stmts);
	if (! event_attached(&self->on_access))
		event_attach(&self->on_access);
	if (! event_attached(&self->on_commit))
		event_attach(&self->on_commit);
}

static inline void
dtr_set_state(Dtr* self, DtrState state)
{
	self->state = state;
}

static inline void
dtr_set_error(Dtr* self, Buf* buf)
{
	assert(! self->error);
	self->error = buf;
}

hot static inline void
dtr_send(Dtr* self, int stmt, ReqList* list)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;

	// first send with snapshot
	if (self->program->snapshot && !self->dispatch.sent)
	{
		// create pipes for all backends
		list_foreach(&self->router->list)
		{
			auto route = list_at(Route, link);
			auto pipe = pipe_create(&self->pipe_cache, route);
			pipe_set_set(set, route->order, pipe);
		}
	}

	// process request list
	auto is_last = (stmt == dispatch->stmt_last);
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
		req_set(req, self->local, self->program, self->args,
		        &self->cte, &self->limit);

		// set pipe per statement backend order and add request to the
		// statement list
		auto ref = dispatch_pipe_set(dispatch, stmt, route->order, pipe);
		req_list_move(list, &ref->req_list, req);

		// add request to the pipe
		pipe_send(pipe, req, stmt, is_last);
		dispatch->sent++;
	}

	// shutdown pipes
	if (is_last)
	{
		list_foreach(&self->router->list)
		{
			auto route = list_at(Route, link);
			auto pipe  = pipe_set_get(set, route->order);
			if (! pipe)
				continue;

			// pipe already involved in the last stmt
			// (with shutdown flag above)
			if (pipe->last_stmt == stmt)
				continue;;

			// add dummy request to the pipe to sync with recv to close pipe
			auto ref = dispatch_pipe_set(dispatch, stmt, route->order, pipe);
			Req* req = req_create(&self->req_cache, REQ_SHUTDOWN);
			req_list_add(&ref->req_list, req);
			pipe_send(pipe, req, stmt, true);
			dispatch->sent++;
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
		defer_buf(error);
		msg_error_rethrow(error);
	}
}

hot static inline void
dtr_shutdown(Dtr* self)
{
	auto set = &self->set;
	auto dispatch = &self->dispatch;
	for (int i = 0; i < set->set_size; i++)
	{
		auto pipe = pipe_set_get(set, i);
		if (! pipe)
			continue;
		if (pipe->state == PIPE_CLOSE)
			continue;

		// send shutdown request
		auto ref = dispatch_pipe_set(dispatch, dispatch->stmt_last, pipe->route->order, pipe);
		Req* req = req_create(&self->req_cache, REQ_SHUTDOWN);
		req_list_add(&ref->req_list, req);
		pipe_send(pipe, req, dispatch->stmt_last, true);
		dispatch->sent++;

		// wait for close
		pipe_close(pipe);
	}
}
