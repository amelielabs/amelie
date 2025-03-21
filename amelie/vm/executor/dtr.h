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
	DtrState state;
	Dispatch dispatch;
	Program* program;
	Result   cte;
	Buf*     error;
	Write    write;
	Event    on_commit;
	Limit    limit;
	Local*   local;
	List     link_commit;
	List     link;
};

static inline void
dtr_init(Dtr* self, Local* local)
{
	self->state   = DTR_NONE;
	self->program = NULL;
	self->error   = NULL;
	self->local   = local;
	dispatch_init(&self->dispatch);
	result_init(&self->cte);
	event_init(&self->on_commit);
	limit_init(&self->limit, var_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_reset(Dtr* self)
{
	dispatch_reset(&self->dispatch);
	result_reset(&self->cte);
	limit_reset(&self->limit, var_int_of(&config()->limit_write));
	write_reset(&self->write);
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	self->state   = DTR_NONE;
	self->program = NULL;
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_free(&self->dispatch);
	result_free(&self->cte);
	event_detach(&self->on_commit);
	write_free(&self->write);
}

static inline void
dtr_create(Dtr* self, Program* program)
{
	self->program = program;
	dispatch_create(&self->dispatch, program->stmts);
	result_create(&self->cte, program->stmts);
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
dtr_send(Dtr* self, int stmt, JobList* list)
{
	scheduler_add(global()->scheduler, &self->dispatch, stmt, list);
	// todo: shutdown for snapshot?
}

hot static inline void
dtr_recv(Dtr* self, int stmt)
{
	dispatch_wait(&self->dispatch, stmt);
}

hot static inline void
dtr_shutdown(Dtr* self)
{
	(void)self;
	// todo
#if 0
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
#endif
}
