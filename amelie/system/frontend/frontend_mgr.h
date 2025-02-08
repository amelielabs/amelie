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

typedef struct FrontendMgr FrontendMgr;

struct FrontendMgr
{
	int       rr;
	int       workers_count;
	Frontend* workers;
};

static inline void
frontend_mgr_init(FrontendMgr* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
frontend_mgr_start(FrontendMgr*  self,
                   FrontendEvent on_connect,
                   void*         on_connect_arg,
                   int           count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = am_malloc(sizeof(Frontend) * count);
	int i = 0;
	for (; i < count; i++)
		frontend_init(&self->workers[i], on_connect, on_connect_arg);
	for (i = 0; i < count; i++)
		frontend_start(&self->workers[i]);
}

static inline void
frontend_mgr_stop(FrontendMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		frontend_stop(&self->workers[i]);
		frontend_free(&self->workers[i]);
	}
	am_free(self->workers);
	self->workers = NULL;
}

static inline int
frontend_mgr_next(FrontendMgr* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
frontend_mgr_forward(FrontendMgr* self, Buf* buf)
{
	assert(self->workers_count > 0);
	int pos = frontend_mgr_next(self);
	frontend_add(&self->workers[pos], buf);
}

static inline void
frontend_mgr_lock(FrontendMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_LOCK, 0);
	}
}

static inline void
frontend_mgr_unlock(FrontendMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_UNLOCK, 0);
	}
}

static inline void
frontend_mgr_sync_users(FrontendMgr* self, UserCache* user_cache)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto fe = &self->workers[i];
		rpc(&fe->task.channel, RPC_SYNC_USERS, 1, user_cache);
	}
}
