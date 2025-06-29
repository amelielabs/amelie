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

typedef struct IoMgr IoMgr;

struct IoMgr
{
	int rr;
	int workers_count;
	Io* workers;
};

static inline void
io_mgr_init(IoMgr* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
io_mgr_start(IoMgr*  self,
             IoEvent on_connect,
             void*   on_connect_arg,
             int     count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Io) * count);
	int i = 0;
	for (; i < count; i++)
		io_init(&self->workers[i], on_connect, on_connect_arg);
	for (i = 0; i < count; i++)
		io_start(&self->workers[i]);
}

static inline void
io_mgr_stop(IoMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		io_stop(&self->workers[i]);
		io_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline int
io_mgr_next(IoMgr* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
io_mgr_forward(IoMgr* self, Buf* buf)
{
	assert(self->workers_count > 0);
	int pos = io_mgr_next(self);
	io_add(&self->workers[pos], buf);
}

static inline void
io_mgr_lock(IoMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_LOCK, 0);
	}
}

static inline void
io_mgr_unlock(IoMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_UNLOCK, 0);
	}
}

static inline void
io_mgr_sync_users(IoMgr* self, UserCache* user_cache)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_SYNC_USERS, 1, user_cache);
	}
}
