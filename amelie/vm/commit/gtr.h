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
	Msg     msg;
	Access* access;
	Tr      tr;
	Usage   usage;
	Write   write;
	Event   on_commit;
	Local*  local;
};

static inline void
gtr_init(Gtr* self)
{
	self->access = NULL;
	self->local  = NULL;
	event_init(&self->on_commit);
	tr_init(&self->tr);
	write_init(&self->write);
	usage_init(&self->usage, "write");
	msg_init(&self->msg, MSG_GTR);
}

static inline void
gtr_reset(Gtr* self)
{
	self->access = NULL;
	self->local  = NULL;
	tr_reset(&self->tr);
	write_reset(&self->write);
	usage_reset(&self->usage);
}

static inline void
gtr_free(Gtr* self)
{
	gtr_reset(self);
	tr_free(&self->tr);
	write_free(&self->write);
}

static inline bool
gtr_lock(Access* self, User* user)
{
	if (! self->count_users)
		return false;

	// same user (shared) or cross-user access (exclusive)
	LockId lock = LOCK_SHARED;
	if (self->count == 1)
	{
		if (access_at_user(self, 0) != &user->rel)
			lock = LOCK_EXCLUSIVE;
	} else {
		lock = LOCK_EXCLUSIVE;
	}

	// lock users
	for (auto i = 0; i < self->count_users; i++)
	{
		auto ref = user_of(access_at_user(self, i));
		lock(&ref->lock, lock);
	}

	return lock == LOCK_EXCLUSIVE;
}

static inline bool
gtr_begin(Gtr* self, Local* local, User* user, Access* access)
{
	self->local = local;

	// set write limit
	auto limits = local->limits;
	if (limits && limits_is_set(limits, LIMIT_WRITE))
		usage_set_limit(&self->usage, true, limits_get(limits, LIMIT_WRITE));

	// set user
	auto tr = &self->tr;
	tr_set_user(tr, &user->rel);
	tr_set_local(tr, local);
	tr_set_limit(tr, &self->usage);

	if (! event_attached(&self->on_commit))
		event_attach(&self->on_commit);

	// set access
	self->access = access;

	// take user locks
	return gtr_lock(access, user);
}
