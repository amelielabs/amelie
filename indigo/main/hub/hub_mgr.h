#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct HubMgr HubMgr;

struct HubMgr
{
	int  rr;
	int  workers_count;
	Hub* workers;
};

static inline void
hub_mgr_init(HubMgr* self)
{
	self->rr            = 0;
	self->workers_count = 0;
	self->workers       = NULL;
}

static inline void
hub_mgr_start(HubMgr* self, Share* share,
              HubIf*  iface,
              int     count)
{
	if (count == 0)
		return;
	self->workers_count = count;
	self->workers = in_malloc(sizeof(Hub) * count);
	int i = 0;
	for (; i < count; i++)
		hub_init(&self->workers[i], share, iface);
	for (i = 0; i < count; i++)
		hub_start(&self->workers[i]);
}

static inline void
hub_mgr_stop(HubMgr* self)
{
	if (self->workers == NULL)
		return;
	for (int i = 0; i < self->workers_count; i++)
   	{
		hub_stop(&self->workers[i]);
		hub_free(&self->workers[i]);
	}
	in_free(self->workers);
	self->workers = NULL;
}

static inline int
hub_mgr_next(HubMgr* self)
{
	assert(self->workers_count > 0);
	if (self->rr == self->workers_count)
		self->rr = 0;
	return self->rr++;
}

static inline void
hub_mgr_forward(HubMgr* self, Buf* buf)
{
	assert(self->workers_count > 0);
	int pos = hub_mgr_next(self);
	hub_add(&self->workers[pos], buf);
}

static inline void
hub_mgr_sync_user_cache(HubMgr* self, UserCache* user_cache)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto hub = &self->workers[i];
		rpc(&hub->task.channel, RPC_USER_CACHE_SYNC, 1, user_cache);
	}
}

static inline void
hub_mgr_session_lock(HubMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto hub = &self->workers[i];
		rpc(&hub->task.channel, RPC_SESSION_LOCK, 0);
	}
}

static inline void
hub_mgr_session_unlock(HubMgr* self)
{
	for (int i = 0; i < self->workers_count; i++)
	{
		auto hub = &self->workers[i];
		rpc(&hub->task.channel, RPC_SESSION_UNLOCK, 0);
	}
}
