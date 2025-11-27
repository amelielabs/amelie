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
	bool     active;
	bool     aborted;
	bool     allocated;
	Log      log;
	Limit*   limit;
	void*    arg;
	List     link;
};

static inline void
tr_init(Tr* self)
{
	self->id        = 0;
	self->active    = false;
	self->aborted   = false;
	self->allocated = false;
	self->limit     = NULL;
	self->arg       = NULL;
	log_init(&self->log);
	list_init(&self->link);
}

static inline void
tr_reset(Tr* self)
{
	self->id      = 0;
	self->active  = false;
	self->aborted = false;
	self->limit   = NULL;
	self->arg     = NULL;
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
tr_set_id(Tr* self, uint64_t id)
{
	self->id = id;
}

static inline void
tr_set_limit(Tr* self, Limit* limit)
{
	self->limit = limit;
}

static inline bool
tr_active(Tr* self)
{
	return self->active;
}

static inline bool
tr_aborted(Tr* self)
{
	return self->aborted;
}

static inline bool
tr_read_only(Tr* self)
{
	return self->log.count == 0;
}
