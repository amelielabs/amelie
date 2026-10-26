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

typedef struct Dispatch Dispatch;
typedef struct Gtr      Gtr;

struct Dispatch
{
	List     ltrs;
	int      ltrs_count;
	Gtr*     gtr;
	Program* program;
	Value*   args;
	Value    result;
	bool     result_has;
	Complete complete;
	Complete ready;
	LtrCache cache;
};

static inline void
dispatch_init(Dispatch* self, Gtr* gtr)
{
	self->ltrs_count = 0;
	self->gtr        = gtr;
	self->program    = NULL;
	self->args       = NULL;
	self->result_has = false;
	list_init(&self->ltrs);
	value_init(&self->result);
	complete_init(&self->complete);
	complete_init(&self->ready);
	ltr_cache_init(&self->cache);
}

static inline void
dispatch_reset(Dispatch* self)
{
	self->program    = NULL;
	self->args       = NULL;
	self->ltrs_count = 0;
	self->result_has = false;
	list_foreach_safe(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		ltr_cache_push(&self->cache, ltr);
	}
	list_init(&self->ltrs);
	value_free(&self->result);
	complete_reset(&self->complete);
	complete_reset(&self->ready);
}

static inline void
dispatch_free(Dispatch* self)
{
	dispatch_reset(self);
	ltr_cache_free(&self->cache);
}

hot static inline void
dispatch_close(Dispatch* self)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->closed)
			continue;
		track_send(&ltr->user->track, &ltr->queue_close);
		ltr->closed = true;
	}
}

hot static inline Ltr*
dispatch_find(Dispatch* self, User* user)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->user == user)
			return ltr;
	}
	return NULL;
}

hot static inline bool
dispatch_overlaps(Dispatch* self, Dispatch* with)
{
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (dispatch_find(with, ltr->user))
			return true;
	}
	return false;
}

hot static inline void
dispatch_prepare(Dispatch* self, Program* program, Value* args)
{
	self->program = program;
	self->args    = args;

	// create ltr for each unique user
	for (auto i = 0; i < program->access.list_count; i++)
	{
		auto ac = access_at(&program->access, i);
		if (ac->rel->type != REL_TABLE)
			continue;

		auto user = table_of(ac->rel)->user;
		auto ltr  = dispatch_find(self, user);
		if (ltr)
			continue;
		ltr = ltr_create(&self->cache, user, self->gtr, self, &self->complete, &self->ready);
		list_append(&self->ltrs, &ltr->link);
		self->ltrs_count++;
	}

	if (self->ltrs_count > 0)
		complete_prepare(&self->complete, self->ltrs_count);

	if (self->ltrs_count > 1)
		complete_prepare(&self->ready, self->ltrs_count);
}

hot static inline void
dispatch_send(Dispatch* self)
{
	if (! self->ltrs_count)
		return;

	list_foreach(&self->ltrs)
	{
		auto ltr   = list_at(Ltr, link);
		auto track = &ltr->user->track;
		ltr->consensus = track->consensus;
		track_send(track, &ltr->msg);
	}
}

hot static inline void
dispatch_wait(Dispatch* self)
{
	if (! self->ltrs_count)
		return;

	// wait for group ready
	complete_wait(&self->ready);
}

hot static inline Buf*
dispatch_complete(Dispatch* self)
{
	if (! self->ltrs_count)
		return NULL;

	// shutdown multi user transaction
	if (self->ltrs_count > 1)
		dispatch_close(self);

	// wait for group completion
	complete_wait(&self->complete);

	// set error
	Buf* error = NULL;
	list_foreach(&self->ltrs)
	{
		auto ltr = list_at(Ltr, link);
		if (ltr->error && !error)
			error = ltr->error;
		auto tr = ltr->tr;
		if (! tr)
			continue;
	}
	return error;
}
