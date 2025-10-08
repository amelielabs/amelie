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
	Msg      msg;
	uint64_t id;
	Dispatch dispatch;
	Program* program;
	Buf*     error;
	bool     abort;
	Write    write;
	Event    on_access;
	Event    on_commit;
	Limit    limit;
	Local*   local;
	CoreMgr* core_mgr;
	Dtr*     link_queue;
	List     link_access;
	List     link;
};

static inline void
dtr_init(Dtr* self, Local* local, CoreMgr* core_mgr)
{
	self->id         = 0;
	self->program    = NULL;
	self->error      = NULL;
	self->abort      = false;
	self->local      = local;
	self->core_mgr   = core_mgr;
	self->link_queue = NULL;
	dispatch_init(&self->dispatch, core_mgr, self);
	event_init(&self->on_access);
	event_init(&self->on_commit);
	limit_init(&self->limit, opt_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_access);
	list_init(&self->link);
	msg_init(&self->msg, MSG_DTR);
}

static inline void
dtr_reset(Dtr* self)
{
	self->id         = 0;
	self->program    = NULL;
	self->link_queue = NULL;
	self->abort      = false;
	if (self->error)
	{
		buf_free(self->error);
		self->error = NULL;
	}
	dispatch_reset(&self->dispatch);
	limit_reset(&self->limit, opt_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_access);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_free(&self->dispatch);
	write_free(&self->write);
}

static inline void
dtr_create(Dtr* self, Program* program)
{
	self->program = program;
	dispatch_prepare(&self->dispatch);
	if (! event_attached(&self->on_access))
		event_attach(&self->on_access);
	if (! event_attached(&self->on_commit))
		event_attach(&self->on_commit);
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

hot static inline void
dtr_send(Dtr* self, ReqList* list, bool last)
{
	// begin local transactions on each core for multi-stmt
	auto dispatch = &self->dispatch;
	if (dispatch_is_first(dispatch))
	{
		int cores_count;
		if (self->program->snapshot)
		{
			dispatch_snapshot(dispatch);
			cores_count = self->core_mgr->cores_count;
		} else {
			cores_count = list->list_count;
		}
		complete_prepare(&dispatch->complete, cores_count);
	}

	// start local transactions and queue requests for execution
	dispatch_send(dispatch, list, last);
}

static inline bool
dtr_active(Dtr* self)
{
	return self->id != 0;
}
