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

typedef struct ServiceQueue ServiceQueue;

struct ServiceQueue
{
	Spinlock        lock;
	List            list;
	List            list_waiters;
	bool            shutdown;
	ServiceReqCache cache;
};

static inline void
service_queue_init(ServiceQueue* self)
{
	self->shutdown = false;
	list_init(&self->list);
	list_init(&self->list_waiters);
	spinlock_init(&self->lock);
	service_req_cache_init(&self->cache);
}

static inline void
service_queue_free(ServiceQueue* self)
{
	service_req_cache_free(&self->cache);
	spinlock_free(&self->lock);
}

static inline void
service_queue_shutdown(ServiceQueue* self)
{
	spinlock_lock(&self->lock);
	// set shutdown and wakeup waiters
	self->shutdown = true;
	list_foreach_safe(&self->list_waiters)
	{
		auto ref = list_at(Action, link);
		list_unlink(&ref->link);
		event_signal(&ref->event);
	}
	spinlock_unlock(&self->lock);
}

hot static inline void
service_queue_wakeup(ServiceQueue* self)
{
	if (list_empty(&self->list_waiters))
		return;
	auto first = list_pop(&self->list_waiters);
	auto ref   = container_of(first, Action, link);
	event_signal(&ref->event);
}

hot static inline void
service_queue_add(ServiceQueue* self, Uuid* id_table)
{
	spinlock_lock(&self->lock);

	ServiceReq* req = NULL;
	list_foreach(&self->list)
	{
		auto at = list_at(ServiceReq, link);
		if (uuid_is(&at->id_table, id_table))
		{
			req = at;
			break;
		}
	}
	if (! req)
	{
		req = service_req_create(&self->cache);
		service_req_set(req, id_table);
		list_append(&self->list, &req->link);
	}
	req->pending++;

	// wakeup one worker
	service_queue_wakeup(self);

	spinlock_unlock(&self->lock);
}

hot static inline bool
service_queue_next(ServiceQueue* self, Action* action)
{
	spinlock_lock(&self->lock);

	// choose next target to work on
	ServiceReq* req = NULL;
	for (;;)
	{
		// shutdown
		if (self->shutdown)
		{
			spinlock_unlock(&self->lock);
			return true;
		}

		list_foreach_safe(&self->list)
		{
			auto at = list_at(ServiceReq, link);
			if (! at->pending)
				continue;
			req = at;
			break;
		}

		if (req)
		{
			// unlink and add to the tail
			list_unlink(&req->link);
			list_append(&self->list, &req->link);
			break;
		}

		// add to the wait list
		event_init(&action->event);
		event_attach(&action->event);
		list_append(&self->list_waiters, &action->link);

		spinlock_unlock(&self->lock);
		event_wait(&action->event, -1);
		spinlock_lock(&self->lock);
	}

	action->req     = req;
	action->pending = req->pending;
	action->type    = ACTION_NONE;
	req->refs++;

	spinlock_unlock(&self->lock);
	return false;
}

static inline void
service_queue_complete(ServiceQueue* self, Action* action)
{
	spinlock_lock(&self->lock);

	auto req = action->req;
	req->refs--;
	assert(req->refs >= 0);

	// if no work been done and the request has no new notifications,
	// set its pending counter to zero to avoid picking it up again
	if (action->type == ACTION_NONE && req->pending <= action->pending)
		req->pending = 0;

	// remove request if it has no active workers and no
	// new notifications
	auto gc = !req->refs && !req->pending;
	if (gc)
		list_unlink(&req->link);

	// wakeup one additional worker, if there is still work to do
	if (! list_empty(&self->list))
		service_queue_wakeup(self);

	spinlock_unlock(&self->lock);

	if (gc)
		service_req_cache_push(&self->cache, req);
}
