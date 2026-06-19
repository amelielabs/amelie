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

typedef struct Gtr Gtr;

struct Gtr
{
	Msg         msg;
	uint64_t    id;
	uint64_t    group;
	uint64_t    group_order;
	DispatchMgr dispatch_mgr;
	Program*    program;
	Buf*        error;
	bool        abort;
	Tr          tr;
	Write       write;
	List        write_cdc;
	Event       on_commit;
	Event*      on_recover;
	Limit       limit;
	Local*      local;
	Gtr*        link_recover;
	Gtr*        link_group;
	List        link_batch;
	List        link;
};

static inline void
gtr_init(Gtr* self, Local* local)
{
	self->id           = 0;
	self->group        = 0;
	self->group_order  = 0;
	self->program      = NULL;
	self->error        = NULL;
	self->abort        = false;
	self->local        = local;
	self->on_recover   = NULL;
	self->link_recover = NULL;
	self->link_group   = NULL;
	dispatch_mgr_init(&self->dispatch_mgr, self);
	event_init(&self->on_commit);
	limit_init(&self->limit, opt_int_of(&config()->limit_write));
	tr_init(&self->tr);
	write_init(&self->write);
	list_init(&self->write_cdc);
	list_init(&self->link_batch);
	list_init(&self->link);
	msg_init(&self->msg, MSG_GTR);
}

static inline void
gtr_reset(Gtr* self)
{
	self->id          = 0;
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
	tr_reset(&self->tr);
	write_reset(&self->write);
	list_init(&self->write_cdc);
	list_init(&self->link_batch);
	list_init(&self->link);
}

static inline void
gtr_free(Gtr* self)
{
	gtr_reset(self);
	dispatch_mgr_free(&self->dispatch_mgr);
	tr_free(&self->tr);
	write_free(&self->write);
}

static inline void
gtr_prepare(Gtr* self, Program* program, User* user)
{
	// set program
	self->program = program;

	// set user
	tr_set_user(&self->tr, &user->rel);

	if (! event_attached(&self->on_commit))
		event_attach(&self->on_commit);
}

static inline bool
gtr_active(Gtr* self)
{
	return self->group != 0;
}

static inline void
gtr_set_abort(Gtr* self)
{
	self->abort = true;
}

static inline void
gtr_set_error(Gtr* self, Buf* buf)
{
	assert(! self->error);
	self->error = buf;
}
