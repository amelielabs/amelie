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

typedef struct Peer  Peer;
typedef struct Peers Peers;

struct Peer
{
	User* user;
};

struct Peers
{
	Buf list;
	int count;
};

static inline Peer*
peers_at(Peers* self, int order)
{
	return &((Peer*)self->list.start)[order];
}

static inline void
peers_init(Peers* self)
{
	self->count = 0;
	buf_init(&self->list);
}

static inline void
peers_reset(Peers* self)
{
	self->count = 0;
	buf_reset(&self->list);
}

static inline void
peers_free(Peers* self)
{
	buf_free(&self->list);
}

static inline bool
peers_empty(Peers* self)
{
	return !self->count;
}

static inline bool
peers_has(Peers* self, User* user)
{
	for (auto i = 0; i < self->count; i++)
		if (peers_at(self, i)->user == user)
			return true;
	return false;
}

hot static inline void
peers_prepare(Peers* self, Access* access)
{
	// track unique users
	for (auto i = 0; i < access->list_count; i++)
	{
		auto rel = access_at(access, i);
		if (rel->rel->type != REL_TABLE)
			continue;
		auto user = table_of(rel->rel)->user;
		if (peers_has(self, user))
			continue;
		auto peer = (Peer*)buf_emplace(&self->list, sizeof(Peer));
		peer->user = user;
		self->count++;
	}
}

hot static inline void
peers_lock(Peers* self, Access* access)
{
	// lock users
	LockId lock = LOCK_SHARED;
	if (access_multi_user(access))
		lock = LOCK_EXCLUSIVE;
	for (auto i = 0; i < self->count; i++)
	{
		auto peer = peers_at(self, i);
		lock(&peer->user->lock, lock);
	}
}
