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

typedef struct HostMgr HostMgr;

struct HostMgr
{
	int   rr;
	int   workers_count;
	Host* workers;
};

static inline void
host_mgr_init(HostMgr* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
host_mgr_start(HostMgr*  self,
               HostEvent on_connect,
               void*     on_connect_arg,
               int       count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Host) * count);
	int i = 0;
	for (; i < count; i++)
		host_init(&self->workers[i], on_connect, on_connect_arg);
	for (i = 0; i < count; i++)
		host_start(&self->workers[i]);
}

static inline void
host_mgr_stop(HostMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		host_stop(&self->workers[i]);
		host_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline int
host_mgr_next(HostMgr* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
host_mgr_forward(HostMgr* self, Buf* buf)
{
	assert(self->workers_count > 0);
	int pos = host_mgr_next(self);
	host_add(&self->workers[pos], buf);
}

static inline void
host_mgr_lock(HostMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_LOCK, 0);
	}
}

static inline void
host_mgr_unlock(HostMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_UNLOCK, 0);
	}
}

static inline void
host_mgr_sync_users(HostMgr* self, UserCache* user_cache)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_SYNC_USERS, 1, user_cache);
	}
}
