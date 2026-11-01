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
	Peers   peers;
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
	peers_init(&self->peers);
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
	peers_reset(&self->peers);
	tr_reset(&self->tr);
	write_reset(&self->write);
	usage_reset(&self->usage);
}

static inline void
gtr_free(Gtr* self)
{
	gtr_reset(self);
	peers_free(&self->peers);
	tr_free(&self->tr);
	write_free(&self->write);
}

static inline bool
gtr_active(Gtr* self)
{
	return !peers_empty(&self->peers);
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

	// prepare peers
	peers_prepare(&self->peers, access);

	// take user locks
	return peers_lock(&self->peers, user);
}
