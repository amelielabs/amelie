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

typedef struct ActionMgr ActionMgr;

struct ActionMgr
{
	Spinlock    lock;
	bool        shutdown;
	List        list;
	List        list_waiters;
	ActionCache cache;
};

static inline void
action_mgr_init(ActionMgr* self)
{
	self->shutdown = false;
	list_init(&self->list);
	list_init(&self->list_waiters);
	spinlock_init(&self->lock);
	action_cache_init(&self->cache);
}

static inline void
action_mgr_free(ActionMgr* self)
{
	action_cache_free(&self->cache);
	spinlock_free(&self->lock);
}

hot static inline void
action_mgr_wakeup(ActionMgr* self, bool all)
{
	list_foreach_safe(&self->list_waiters)
	{
		auto waiter = list_at(ActionWaiter, link);
		list_unlink(&waiter->link);
		event_signal(&waiter->event);
		if (! all)
			break;
	}
}

static inline void
action_mgr_shutdown(ActionMgr* self)
{
	spinlock_lock(&self->lock);
	self->shutdown = true;
	action_mgr_wakeup(self, true);
	spinlock_unlock(&self->lock);
}

hot static inline void
action_mgr_create(ActionMgr* self, int type, uint64_t id)
{
	spinlock_lock(&self->lock);

	Action* action = NULL;
	list_foreach(&self->list)
	{
		auto at = list_at(Action, link);
		if (at->type == type && at->id == id)
		{
			action = at;
			break;
		}
	}
	if (! action)
	{
		action = action_create(&self->cache);
		action->type = type;
		action->id   = id;
		list_append(&self->list, &action->link);
	}

	// wakeup one worker at a time
	if (! action->in_progress)
		action_mgr_wakeup(self, false);

	spinlock_unlock(&self->lock);
}

hot static inline Action*
action_mgr_next(ActionMgr* self)
{
	spinlock_lock(&self->lock);

	Action* action = NULL;
	for (;;)
	{
		// shutdown
		if (self->shutdown)
			break;

		// pick next action
		list_foreach(&self->list)
		{
			auto at = list_at(Action, link);
			if (at->in_progress)
				continue;
			action = at;
			break;
		}

		if (action)
		{
			action->in_progress = true;
			break;
		}

		// waiter for the next event
		ActionWaiter waiter;
		action_waiter_init(&waiter);
		list_append(&self->list_waiters, &waiter.link);

		spinlock_unlock(&self->lock);
		event_wait(&waiter.event, -1);
		spinlock_lock(&self->lock);
	}

	spinlock_unlock(&self->lock);
	return action;
}

static inline void
action_mgr_complete(ActionMgr* self, Action* action)
{
	spinlock_lock(&self->lock);
	list_unlink(&action->link);
	spinlock_unlock(&self->lock);
	action_cache_push(&self->cache, action);
}
