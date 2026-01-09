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

struct Dtr
{
	Msg         msg;
	uint64_t    group;
	uint64_t    group_order;
	DispatchMgr dispatch_mgr;
	Program*    program;
	Lock        lock;
	Buf*        error;
	bool        abort;
	Write       write;
	Event       on_commit;
	Limit       limit;
	Local*      local;
	Dtr*        link_group;
	List        link_batch;
	List        link;
};

static inline void
dtr_init(Dtr* self, Local* local)
{
	self->group       = 0;
	self->group_order = 0;
	self->program     = NULL;
	self->error       = NULL;
	self->abort       = false;
	self->local       = local;
	self->link_group  = NULL;
	dispatch_mgr_init(&self->dispatch_mgr, self);
	lock_init(&self->lock);
	event_init(&self->on_commit);
	limit_init(&self->limit, opt_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_batch);
	list_init(&self->link);
	msg_init(&self->msg, MSG_DTR);
}

static inline void
dtr_reset(Dtr* self)
{
	self->group       = 0;
	self->group_order = 0;
	self->program     = NULL;
	self->abort       = false;
	self->link_group  = NULL;
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	dispatch_mgr_reset(&self->dispatch_mgr);
	limit_reset(&self->limit, opt_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_batch);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_mgr_free(&self->dispatch_mgr);
	write_free(&self->write);
}

static inline void
dtr_create(Dtr* self, Program* program)
{
	self->program = program;
	if (! event_attached(&self->on_commit))
		event_attach(&self->on_commit);
	lock_set(&self->lock, &program->access);
}

static inline bool
dtr_active(Dtr* self)
{
	return self->group != 0;
}

static inline void
dtr_set_abort(Dtr* self)
{
	self->abort = true;
}

static inline void
dtr_set_error(Dtr* self, Buf* buf)
{
	assert(! self->error);
	self->error = buf;
}
