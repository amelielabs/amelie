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
	uint64_t    id;
	DispatchMgr dispatch_mgr;
	Program*    program;
	Buf*        error;
	bool        abort;
	Write       write;
	Event       on_access;
	Event       on_commit;
	Limit       limit;
	Local*      local;
	CoreMgr*    core_mgr;
	Dtr*        link_queue;
	List        link_access;
	List        link;
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
	dispatch_mgr_init(&self->dispatch_mgr, core_mgr, self);
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
	dispatch_mgr_reset(&self->dispatch_mgr);
	limit_reset(&self->limit, opt_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_access);
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
	dispatch_mgr_prepare(&self->dispatch_mgr);
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
	// begin local transactions for each core to handle
	auto sent = self->dispatch_mgr.list_count;
	if (sent == 0 && self->program->snapshot)
		dispatch_mgr_snapshot(&self->dispatch_mgr);

	// start local transactions and queue requests for execution
	dispatch_mgr_send(&self->dispatch_mgr, list, last);

	// finilize still active transactions on the last send
	if (last)
		dispatch_mgr_close(&self->dispatch_mgr);
}

hot static inline void
dtr_recv(Dtr* self)
{
	dispatch_mgr_recv(&self->dispatch_mgr);
}

static inline bool
dtr_active(Dtr* self)
{
	return self->id != 0;
}
