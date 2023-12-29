#pragma once

//
// indigo
//
// SQL OLTP database
//

typedef struct WalEvent    WalEvent;
typedef struct WalEventMgr WalEventMgr;

struct WalEvent
{
	uint64_t   lsn;
	Condition* event;
	bool       attached;
	List       link;
};

struct WalEventMgr
{
	Spinlock lock;
	List     list;
	int      list_count;
};

static inline void
wal_event_init(WalEvent* self)
{
	self->lsn      = 0;
	self->event    = NULL;
	self->attached = false;
	list_init(&self->link);
}

static inline void
wal_event_attach(WalEvent* self)
{
	if (self->event == NULL)
		self->event = condition_create();
}

static inline void
wal_event_detach(WalEvent* self)
{
	if (self->event)
	{
		condition_free(self->event);
		self->event = NULL;
	}
}

static inline void
wal_event_mgr_init(WalEventMgr* self)
{
	self->list_count = 0;
	list_init(&self->list);
	spinlock_init(&self->lock);
}

static inline void
wal_event_mgr_free(WalEventMgr* self)
{
	assert(! self->list_count);
	spinlock_free(&self->lock);
}

static inline bool
wal_event_mgr_wait(WalEventMgr* self, WalEvent* event, uint64_t lsn,
                   int time_ms)
{
	// add event to the wait list
	spinlock_lock(&self->lock);

	// return if current lsn is up to date
	if (lsn > 0 && config_lsn() >= lsn)
	{
		spinlock_unlock(&self->lock);
		return false;
	}

	event->lsn      = lsn;
	event->attached = true;
	list_append(&self->list, &event->link);
	self->list_count++;

	spinlock_unlock(&self->lock);

	// wait for signal
	coroutine_cancel_pause(mn_self());
	bool timedout;
	timedout = condition_wait(event->event, time_ms);

	// dettach
	spinlock_lock(&self->lock);

	event->attached = false;
	list_unlink(&event->link);
	self->list_count--;
	assert(self->list_count >= 0);

	spinlock_unlock(&self->lock);

	// cancellation point
	coroutine_cancel_resume(mn_self());
	return timedout;
}

static inline void
wal_event_mgr_signal(WalEventMgr* self, uint64_t lsn)
{
	spinlock_lock(&self->lock);

	list_foreach(&self->list) {
		auto event = list_at(WalEvent, link);
		if (event->lsn <= lsn)
			condition_signal(event->event);
	}

	spinlock_unlock(&self->lock);
}
