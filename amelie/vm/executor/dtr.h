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
	uint64_t    id;
	DtrState    state;
	DispatchMgr dispatch_mgr;
	Program*    program;
	Buf*        error;
	Write       write;
	Event       on_access;
	Event       on_commit;
	Limit       limit;
	Local*      local;
	CoreMgr*    core_mgr;
	List        link_prepare;
	List        link_access;
	List        link;
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
	dispatch_mgr_init(&self->dispatch_mgr, core_mgr, self);
	event_init(&self->on_access);
	event_init(&self->on_commit);
	limit_init(&self->limit, opt_int_of(&config()->limit_write));
	write_init(&self->write);
	list_init(&self->link_prepare);
	list_init(&self->link_access);
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
	dispatch_mgr_reset(&self->dispatch_mgr);
	limit_reset(&self->limit, opt_int_of(&config()->limit_write));
	write_reset(&self->write);
	list_init(&self->link_prepare);
	list_init(&self->link_access);
	list_init(&self->link);
}

static inline void
dtr_free(Dtr* self)
{
	dtr_reset(self);
	dispatch_mgr_free(&self->dispatch_mgr);
	event_detach(&self->on_access);
	event_detach(&self->on_commit);
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
dtr_send(Dtr* self, ReqList* list)
{
	// begin local transactions for each core to handle
	auto sent = self->dispatch_mgr.list_count;
	if (sent == 0 && self->program->snapshot)
		dispatch_mgr_snapshot(&self->dispatch_mgr);

	// start local transactions and queue requests for execution
	auto last = self->program->sends == (sent + 1);
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
