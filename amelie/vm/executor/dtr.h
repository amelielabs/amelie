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
	DTR_LOCK,
	DTR_BEGIN,
	DTR_PREPARE,
	DTR_ERROR,
	DTR_COMMIT,
	DTR_ABORT
} DtrState;

struct Dtr
{
	uint64_t id;
	bool     exclusive;
	DtrState state;
	Dispatch dispatch;
	Program* program;
	Result   cte;
	Buf*     error;
	Write    write;
	Event    on_lock;
	Event    on_commit;
	Limit    limit;
	Local*   local;
	List     link_commit;
	List     link;
};

static inline void
dtr_init(Dtr* self, Local* local)
{
	self->state     = DTR_NONE;
	self->exclusive = false;
	self->id        = 0;
	self->program   = NULL;
	self->error     = NULL;
	self->local     = local;
	dispatch_init(&self->dispatch);
	result_init(&self->cte);
	event_init(&self->on_lock);
	event_init(&self->on_commit);
	limit_init(&self->limit, var_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_reset(Dtr* self)
{
	self->state     = DTR_NONE;
	self->id        = 0;
	self->program   = NULL;
	self->exclusive = false;
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	dispatch_reset(&self->dispatch);
	result_reset(&self->cte);
	limit_reset(&self->limit, var_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_commit);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_free(&self->dispatch);
	result_free(&self->cte);
	event_detach(&self->on_lock);
	event_detach(&self->on_commit);
	write_free(&self->write);
}

static inline void
dtr_create(Dtr* self, Program* program)
{
	self->program = program;
	self->exclusive = program->exclusive;
	dispatch_create(&self->dispatch, program->stmts);
	result_create(&self->cte, program->stmts);
	if (! event_attached(&self->on_lock))
		event_attach(&self->on_lock);
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
req_complete(Req* self)
{
	// send OK or ERROR based on the req result
	auto dtr = self->arg.dtr;
	if (dtr)
	{
		auto step = dispatch_at(&dtr->dispatch, self->arg.step);
		if (self->arg.error)
			atomic_u32_inc(&step->errors);
		atomic_u32_inc(&step->complete);
		event_signal(&dtr->dispatch.on_complete);
	} else
	{
		// commit/abort (async)
		req_free(self);
	}
}
