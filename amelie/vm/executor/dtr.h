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
	uint64_t id;
	DtrState state;
	Dispatch dispatch;
	Program* program;
	Result   cte;
	Buf*     error;
	Write    write;
	Event    on_commit;
	Limit    limit;
	Local*   local;
	CoreMgr* core_mgr;
	List     link_prepare;
	List     link;
};

static inline void
dtr_init(Dtr* self, Local* local, CoreMgr* core_mgr)
{
	self->id       = 0;
	self->state    = DTR_NONE;
	self->program  = NULL;
	self->error    = NULL;
	self->local    = local;
	self->core_mgr = core_mgr;
	dispatch_init(&self->dispatch, core_mgr);
	result_init(&self->cte);
	event_init(&self->on_commit);
	limit_init(&self->limit, var_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_prepare);
	list_init(&self->link);
}

static inline void
dtr_reset(Dtr* self)
{
	self->id      = 0;
	self->state   = DTR_NONE;
	self->program = NULL;
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	dispatch_reset(&self->dispatch);
	result_reset(&self->cte);
	limit_reset(&self->limit, var_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_prepare);
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
	dispatch_create(&self->dispatch, self, program->stmts);
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
dtr_send(Dtr* self, int step, ReqList* list)
{
	// begin local transactions for each core to handle
	if (step == 0 && self->program->exclusive)
		dispatch_add_snapshot(&self->dispatch);

	// add requests to the dispatch step,
	// start local transactions and queue requests for execution
	list_foreach_safe(&list->list)
	{
		auto req = list_at(Req, link);
		req_list_del(list, req);
		dispatch_add(&self->dispatch, step, req);
	}

	// finilize still active transactions on the last step
	if (step == self->program->stmts_last)
		dispatch_close(&self->dispatch);
}

hot static inline void
dtr_recv(Dtr* self, int step)
{
	dispatch_wait(&self->dispatch, step);
}
