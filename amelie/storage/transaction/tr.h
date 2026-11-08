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

typedef struct Tr     Tr;
typedef struct Grants Grants;

struct Tr
{
	Log     log;
	Grants* grants;
	Rel*    user_;
	Local*  local;
	Usage*  write;
	bool    allocated;
	List    link;
};

static inline void
tr_init(Tr* self)
{
	self->grants    = NULL;
	self->user_     = NULL;
	self->local     = NULL;
	self->write     = NULL;
	self->allocated = false;
	log_init(&self->log);
	list_init(&self->link);
}

static inline void
tr_reset(Tr* self)
{
	self->grants = NULL;
	self->user_  = NULL;
	self->local  = NULL;
	self->write  = NULL;
	log_reset(&self->log);
	list_init(&self->link);
}

static inline Tr*
tr_allocate(void)
{
	auto self = (Tr*)am_malloc(sizeof(Tr));
	tr_init(self);
	self->allocated = true;
	return self;
}

static inline void
tr_free(Tr* self)
{
	log_free(&self->log);
	if (self->allocated)
		am_free(self);
}

always_inline static inline bool
tr_active(Tr* self)
{
	return self->log.count > 0;
}

static inline void
tr_set_local(Tr* self, Local* local)
{
	self->local = local;
}

static inline void
tr_set_limit(Tr* self, Usage* write)
{
	self->write = write;
}

static inline void
tr_set_grants(Tr* self, Grants* grants)
{
	self->grants = grants;
}

static inline void
tr_set_user(Tr* self, Rel* user)
{
	self->user_ = user;
}

hot static inline void
tr_commit(Tr* self)
{
	if (! tr_active(self))
		return;

	auto log = &self->log;
	for (int pos = 0; pos < log->count; pos++)
	{
		auto op = log_of(log, pos);
		if (op->iface)
			op->iface->commit(log, op);
	}

	// reset log
	log_reset(log);
}

static inline void
tr_abort(Tr* self)
{
	if (! tr_active(self))
		return;

	auto log = &self->log;
	for (int pos = self->log.count - 1; pos >= 0; pos--)
	{
		auto op = log_of(log, pos);
		if (op->iface)
			op->iface->abort(log, op);
	}

	// reset log
	log_reset(log);
}
