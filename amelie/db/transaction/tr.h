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

typedef struct Tr Tr;

struct Tr
{
	uint64_t id;
	Log      log;
	Limit*   limit;
	Rel*     user;
	bool     active;
	bool     aborted;
	bool     allocated;
	List     link;
};

static inline void
tr_init(Tr* self)
{
	self->id        = 0;
	self->limit     = NULL;
	self->user      = NULL;
	self->active    = false;
	self->aborted   = false;
	self->allocated = false;
	log_init(&self->log);
	list_init(&self->link);
}

static inline void
tr_reset(Tr* self)
{
	self->id      = 0;
	self->limit   = NULL;
	self->user    = NULL;
	self->active  = false;
	self->aborted = false;
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

static inline void
tr_set_id(Tr* self, uint64_t value)
{
	self->id = value;
}

static inline void
tr_set_limit(Tr* self, Limit* limit)
{
	self->limit = limit;
}

static inline void
tr_set_user(Tr* self, Rel* user)
{
	self->user = user;
}

always_inline static inline bool
tr_active(Tr* self)
{
	return self->active;
}

always_inline static inline bool
tr_aborted(Tr* self)
{
	return self->aborted;
}

always_inline static inline bool
tr_pending(Tr* self)
{
	return self->log.count > 0;
}

always_inline static inline bool
tr_persists(Tr* self)
{
	return self->log.count > 0 && !write_log_empty(&self->log.write_log);
}
